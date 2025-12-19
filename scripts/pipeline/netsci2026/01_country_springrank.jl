using CSV
using DataFrames
using YAML
using OSRExamples
using OSRExamples.MetaUtils
using OSRExamples.FlowUtils
using OSRExamples.RunConfigUtils
using OrcidSpringRank
using Statistics

function parse_args(args::Vector{String})
    input_root = nothing
    output_root = joinpath("out", "netsci2026")
    tag = nothing
    config = joinpath("papers", "netsci2026", "config", "01_country_springrank.yaml")
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

function main()
    input_root, output_root, tag, config_path, memory, threads, force = parse_args(ARGS)

    out_dir = joinpath(output_root, tag, "01_country_springrank")
    mkpath(out_dir)

    out_csv = joinpath(out_dir, "springrank.csv")
    out_meta = joinpath(out_dir, "_meta.json")

    if isfile(out_csv) && !force
        error("output exists: $out_csv (use --force to overwrite)")
    end

    cfg = OrcidSpringRank.load_cfg(config_path)

    ranks = OrcidSpringRank.compute(input_root, cfg; memory = memory, threads = threads)
    rename!(
        ranks,
        Dict(
            :node_id => :country_node_id,
            :node => :country_alpha2,
            :score => :springrank_score,
        ),
    )

    raw = YAML.load_file(config_path)
    haskey(raw, "flow") || error("01 config missing key: flow")
    fr = raw["flow"]
    haskey(fr, "edge") || error("01 config.flow missing key: edge")
    haskey(fr, "sql") || error("01 config.flow missing key: sql")

    tmp = mktempdir()
    flow_yaml = joinpath(tmp, "flow.yaml")
    RunConfigUtils.write_query_yaml(flow_yaml, String(fr["edge"]), String(fr["sql"]))
    flow_cfg = OrcidSpringRank.load_cfg(flow_yaml)

    edges =
        OrcidSpringRank.query_df(input_root, flow_cfg; memory = memory, threads = threads)
    rm(tmp; recursive = true, force = true)

    flows = FlowUtils.country_flows(
        edges;
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

    ord = sortperm(res.springrank_score; rev = true)
    res = res[ord, :]
    res[!, :springrank_rank] = collect(1:nrow(res))

    CSV.write(out_csv, res)

    base_meta_path = joinpath(output_root, tag, "_meta.json")
    upstream = isfile(base_meta_path) ? MetaUtils.read_json(base_meta_path) : Dict()

    meta = Dict(
        "timestamp_utc" => string(MetaUtils.now_utc()),
        "pipeline" => "netsci2026/01_country_springrank",
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
        ),
    )

    MetaUtils.write_json(out_meta, meta)
    println(out_dir)
end

main()
