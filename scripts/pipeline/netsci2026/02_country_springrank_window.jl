using CSV
using DataFrames
using DBInterface
using DuckDB
using OSRExamples
using OSRExamples.FlowUtils
using OSRExamples.MetaUtils
using OSRExamples.WindowUtils
using OrcidSpringRank
using Statistics
using YAML

function parse_args(args::Vector{String})
    input_root = nothing
    output_root = joinpath("out", "netsci2026")
    tag = nothing
    config = joinpath("papers", "netsci2026", "config", "02_country_springrank_window.yaml")
    memory = get(ENV, "DUCKDB_MEM", "16GB")
    threads = Threads.nthreads()
    force = false

    i = 1
    while i <= length(args)
        a = args[i]
        if a == "--input-root"
            i += 1
            i <= length(args) || error("missing value for --input-root")
            input_root = args[i]
        elseif a == "--output-root"
            i += 1
            i <= length(args) || error("missing value for --output-root")
            output_root = args[i]
        elseif a == "--tag"
            i += 1
            i <= length(args) || error("missing value for --tag")
            tag = args[i]
        elseif a == "--config"
            i += 1
            i <= length(args) || error("missing value for --config")
            config = args[i]
        elseif a == "--memory"
            i += 1
            i <= length(args) || error("missing value for --memory")
            memory = args[i]
        elseif a == "--threads"
            i += 1
            i <= length(args) || error("missing value for --threads")
            threads = parse(Int, args[i])
        elseif a == "--force"
            force = true
        else
            error("unknown arg: $a")
        end
        i += 1
    end

    input_root === nothing && error("required: --input-root")

    input_root = abspath(String(input_root))
    output_root = abspath(String(output_root))
    config = abspath(String(config))
    tag = tag === nothing ? basename(input_root) : String(tag)

    return input_root, output_root, tag, config, memory, threads, force
end

function render_sql(sql::String; year_center::Int, tmin::Int, tmax::Int, time_field::String)
    s = String(sql)
    s = replace(s, "{{year}}" => string(year_center))
    s = replace(s, "{{tmin}}" => string(Float64(tmin)))
    s = replace(s, "{{tmax}}" => string(Float64(tmax)))
    s = replace(s, "{{time_field}}" => time_field)
    return s
end

function write_cfg(path::String; λv, methodv)
    open(path, "w") do io
        write(io, "edge: window_edges\n")
        write(io, "sql: |\n")
        write(io, "  SELECT src, dst, w_rank AS w\n")
        write(io, "  FROM edge\n")
        write(io, "  WHERE w_rank > 0\n\n")
        write(io, "compute:\n")
        write(io, "  alg: springrank\n")
        write(io, "  λ: $(λv)\n")
        write(io, "  method: $(methodv)\n")
        write(io, "  src: src\n")
        write(io, "  dst: dst\n")
        write(io, "  w: w\n")
    end
    return path
end

function write_flow_cfg(path::String)
    open(path, "w") do io
        write(io, "edge: window_edges\n")
        write(io, "sql: |\n")
        write(io, "  SELECT src, dst, w\n")
        write(io, "  FROM edge\n")
    end
    return path
end

function main()
    input_root, output_root, tag, config_path, memory, threads, force = parse_args(ARGS)

    raw = YAML.load_file(config_path)
    edge_name = String(raw["edge"])
    win = raw["window"]
    time_field = String(win["time_field"])
    year_from = Int(win["year_from"])
    year_to = Int(win["year_to"])
    width = Int(win["width"])
    mode = Symbol(String(win["mode"]))
    step = haskey(win, "step") ? Int(win["step"]) : 1
    sql_tpl = String(raw["sql"])

    comp = raw["compute"]
    λv = haskey(comp, "λ") ? comp["λ"] : (haskey(comp, "lambda") ? comp["lambda"] : 1e-8)
    methodv = haskey(comp, "method") ? comp["method"] : "auto"

    out_dir = joinpath(output_root, tag, "02_country_springrank_window")
    mkpath(out_dir)
    edges_dir = joinpath(out_dir, "edges")
    mkpath(edges_dir)

    out_csv = joinpath(out_dir, "springrank_windowed.csv")
    out_meta = joinpath(out_dir, "_meta.json")

    if isfile(out_csv) && !force
        error("output exists: $out_csv (use --force to overwrite)")
    end

    edge_parquet = joinpath(input_root, "edges", "$(edge_name).parquet")
    isfile(edge_parquet) || error("edge parquet not found: $edge_parquet")

    con = DBInterface.connect(DuckDB.DB, ":memory:")
    DBInterface.execute(con, "SET memory_limit = '$(memory)';")
    DBInterface.execute(con, "SET threads = $(threads);")
    DBInterface.execute(
        con,
        "CREATE OR REPLACE TEMP VIEW edge AS SELECT * FROM read_parquet('$(edge_parquet)');",
    )

    rows = DataFrame[]
    years = collect(year_from:step:year_to)

    for y in years
        start_y, end_excl_y = WindowUtils.window_bounds_year(y, width; mode = mode)

        out_edge = joinpath(edges_dir, "window_year=$(y).parquet")
        if isfile(out_edge)
            if force
                rm(out_edge; force = true)
            end
        end

        if !isfile(out_edge)
            q = render_sql(
                sql_tpl;
                year_center = y,
                tmin = start_y,
                tmax = end_excl_y,
                time_field = time_field,
            )
            DBInterface.execute(con, "COPY ($(q)) TO '$(out_edge)' (FORMAT parquet);")
        end

        tmp_root = mktempdir()
        mkpath(joinpath(tmp_root, "edges"))
        cp(out_edge, joinpath(tmp_root, "edges", "window_edges.parquet"); force = true)

        cfg_path = joinpath(tmp_root, "run.yaml")
        flow_cfg_path = joinpath(tmp_root, "flow.yaml")
        write_cfg(cfg_path; λv = λv, methodv = methodv)
        write_flow_cfg(flow_cfg_path)

        cfg = OrcidSpringRank.load_cfg(cfg_path)
        ranks = OrcidSpringRank.compute(tmp_root, cfg; memory = memory, threads = threads)
        rename!(
            ranks,
            Dict(
                :node_id => :country_node_id,
                :node => :country_alpha2,
                :score => :springrank_score,
            ),
        )

        cfg_flow = OrcidSpringRank.load_cfg(flow_cfg_path)
        e = OrcidSpringRank.query_df(tmp_root, cfg_flow; memory = memory, threads = threads)
        flows = FlowUtils.country_flows(
            e;
            src = :src,
            dst = :dst,
            w = :w,
            key = :country_alpha2,
        )

        res = leftjoin(ranks, flows, on = :country_alpha2)
        for c in (:flow_in, :flow_out, :flow_self, :flow_total)
            if !hasproperty(res, c)
                res[!, c] = zeros(Float64, nrow(res))
            else
                replace!(res[!, c], missing => 0)
                res[!, c] = Float64.(res[!, c])
            end
        end

        res[!, :year_center] .= y
        res[!, :window_start_year] .= start_y
        res[!, :window_end_year_excl] .= end_excl_y
        res[!, :window_width] .= width
        res[!, :window_mode] .= String(mode)

        ord = sortperm(res.springrank_score; rev = true)
        res = res[ord, :]
        res[!, :springrank_rank] = collect(1:nrow(res))

        push!(rows, res)
        rm(tmp_root; recursive = true, force = true)
    end

    DBInterface.close!(con)

    all = vcat(rows...)
    CSV.write(out_csv, all)

    base_meta_path = joinpath(output_root, tag, "_meta.json")
    upstream = isfile(base_meta_path) ? MetaUtils.read_json(base_meta_path) : Dict()

    meta = Dict(
        "timestamp_utc" => string(MetaUtils.now_utc()),
        "pipeline" => "netsci2026/02_country_springrank_window",
        "params" => Dict(
            "tag" => tag,
            "input_root" => input_root,
            "output_root" => output_root,
            "config" => config_path,
            "memory" => memory,
            "threads" => threads,
        ),
        "upstream" => Dict("base_meta_path" => base_meta_path, "base_meta" => upstream),
        "hashes" => Dict(
            "config_sha256" => MetaUtils.hash_file_or_empty(config_path),
            "out_csv_sha256" => MetaUtils.hash_file_or_empty(out_csv),
            "edges_dir_sha256" => MetaUtils.hash_tree(edges_dir; exts = [".parquet"]),
        ),
    )

    MetaUtils.write_json(out_meta, meta)
    println(out_dir)
end

main()
