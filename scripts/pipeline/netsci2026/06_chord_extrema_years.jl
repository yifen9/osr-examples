using CSV
using DataFrames
using YAML
using CairoMakie
using OSRExamples
using OSRExamples.MetaUtils
using OSRExamples.ChordUtils
using OSRExamples.ChordPlotUtils

function parse_args(args::Vector{String})
    output_root = joinpath("out", "netsci2026")
    tag = nothing
    config = joinpath("papers", "netsci2026", "config", "06_chord_extrema_years.yaml")
    force = false

    i = 1
    while i <= length(args)
        a = args[i]
        if a == "--output-root"
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
        elseif a == "--force"
            force = true
        else
            error("unknown arg: $a")
        end
        i += 1
    end

    tag === nothing && error("required: --tag")
    output_root = abspath(String(output_root))
    tag = String(tag)
    config = abspath(String(config))
    return output_root, tag, config, force
end

_as_sym(x) = Symbol(String(x))

function _req(d, k::String)
    haskey(d, k) || error("missing key: $(k)")
    return d[k]
end

function _read_keep_nodes(path::String, col::Symbol)
    df = DataFrame(CSV.File(path))
    hasproperty(df, col) || error("keep_nodes csv missing column: $(col)")
    v = String.(df[!, col])
    isempty(v) && error("keep_nodes is empty: $(path)")
    return sort(unique(v))
end

function _read_extrema(path::String)
    df = DataFrame(CSV.File(path))
    for c in (:kind, :year_center, :window_start_year, :window_end_year_excl)
        hasproperty(df, c) || error("extrema csv missing column: $(c)")
    end
    df[!, :kind] = String.(df.kind)
    df[!, :year_center] = Int.(df.year_center)
    df[!, :window_start_year] = Int.(df.window_start_year)
    df[!, :window_end_year_excl] = Int.(df.window_end_year_excl)
    return df
end

function _parquet_for_year(step02_edges_dir::String, year_center::Int)
    p = joinpath(step02_edges_dir, "window_year=$(year_center).parquet")
    isfile(p) || error("missing window parquet: $(p)")
    return p
end

function _order_by_springrank(
    path::String;
    year::Int,
    keep::Set{String},
    c_year::Symbol,
    c_country::Symbol,
    c_score::Symbol,
    desc::Bool,
)
    df = DataFrame(CSV.File(path))
    for c in (c_year, c_country, c_score)
        hasproperty(df, c) || error("missing column in springrank_windowed.csv: $(c)")
    end
    df = df[df[!, c_year] .== year, :]
    df[!, c_country] = String.(df[!, c_country])
    df[!, c_score] = Float64.(df[!, c_score])
    df = df[in.(df[!, c_country], Ref(keep)), :]
    ord = sortperm(df[!, c_score]; rev = desc)
    v = df[ord, c_country]
    v = String.(v)
    isempty(v) && error("empty springrank order for year=$(year)")
    return v
end

function _save_both(svg_path::String, png_path::String, fig)
    save(svg_path, fig)
    save(png_path, fig)
end

function main()
    output_root, tag, config_path, force = parse_args(ARGS)
    cfg = YAML.load_file(config_path)

    up = _req(cfg, "upstream")
    step02_dir = String(_req(up, "step02_dir"))
    step02_edges_subdir = String(_req(up, "step02_edges_subdir"))
    step02_csv = String(_req(up, "step02_csv"))
    step03_dir = String(_req(up, "step03_dir"))
    keep_nodes_csv = String(_req(up, "keep_nodes_csv"))
    step05_dir = String(_req(up, "step05_dir"))
    extrema_csv = String(_req(up, "extrema_csv"))

    cols = _req(cfg, "columns")
    keep_nodes_col = _as_sym(_req(cols, "keep_nodes_col"))
    c_year = _as_sym(_req(cols, "year_center"))
    c_country = _as_sym(_req(cols, "country"))
    c_score = _as_sym(_req(cols, "springrank_score"))

    io_cfg = _req(cfg, "io")
    step06_dir = String(_req(io_cfg, "step06_dir"))
    edges_max_csv_out = String(_req(io_cfg, "edges_max_csv"))
    edges_min_csv_out = String(_req(io_cfg, "edges_min_csv"))
    svg_max = String(_req(io_cfg, "svg_max"))
    svg_min = String(_req(io_cfg, "svg_min"))
    png_max = String(_req(io_cfg, "png_max"))
    png_min = String(_req(io_cfg, "png_min"))

    draw_cfg = _req(cfg, "draw")
    keep_top_edges = Int(_req(draw_cfg, "keep_top_edges"))
    edges_w_col = _as_sym(_req(draw_cfg, "edges_weight_col"))
    order_desc = Bool(_req(draw_cfg, "order_desc"))
    title_prefix = String(_req(draw_cfg, "title_prefix"))

    chord_cfg = _req(cfg, "chord")

    base_dir = joinpath(output_root, tag)
    step02_edges_dir = joinpath(base_dir, step02_dir, step02_edges_subdir)
    step02_csv_path = joinpath(base_dir, step02_dir, step02_csv)
    step03_keep_nodes_path = joinpath(base_dir, step03_dir, keep_nodes_csv)
    step05_extrema_path = joinpath(base_dir, step05_dir, extrema_csv)

    isdir(step02_edges_dir) || error("missing dir: $(step02_edges_dir)")
    isfile(step02_csv_path) || error("missing input: $(step02_csv_path)")
    isfile(step03_keep_nodes_path) || error("missing input: $(step03_keep_nodes_path)")
    isfile(step05_extrema_path) || error("missing input: $(step05_extrema_path)")

    out_dir = joinpath(base_dir, step06_dir)
    mkpath(out_dir)

    p_edges_max = joinpath(out_dir, edges_max_csv_out)
    p_edges_min = joinpath(out_dir, edges_min_csv_out)
    p_svg_max = joinpath(out_dir, svg_max)
    p_svg_min = joinpath(out_dir, svg_min)
    p_png_max = joinpath(out_dir, png_max)
    p_png_min = joinpath(out_dir, png_min)
    p_meta = joinpath(out_dir, "_meta.json")

    if (isfile(p_svg_max) || isfile(p_svg_min) || isfile(p_png_max) || isfile(p_png_min)) &&
       !force
        error("outputs exist in: $(out_dir) (use --force to overwrite)")
    end

    keep_nodes = _read_keep_nodes(step03_keep_nodes_path, keep_nodes_col)
    keep_set = Set(keep_nodes)
    ex = _read_extrema(step05_extrema_path)

    i_max = findfirst(==("max"), ex.kind)
    i_min = findfirst(==("min"), ex.kind)
    i_max === nothing && error("missing kind=max in extrema csv")
    i_min === nothing && error("missing kind=min in extrema csv")

    y_max = Int(ex.year_center[i_max])
    y_min = Int(ex.year_center[i_min])

    pq_max = _parquet_for_year(step02_edges_dir, y_max)
    pq_min = _parquet_for_year(step02_edges_dir, y_min)

    order_max = _order_by_springrank(
        step02_csv_path;
        year = y_max,
        keep = keep_set,
        c_year = c_year,
        c_country = c_country,
        c_score = c_score,
        desc = order_desc,
    )
    order_min = _order_by_springrank(
        step02_csv_path;
        year = y_min,
        keep = keep_set,
        c_year = c_year,
        c_country = c_country,
        c_score = c_score,
        desc = order_desc,
    )

    emax = ChordUtils.load_edges_parquet_agg(
        pq_max,
        keep_nodes;
        src = :src,
        dst = :dst,
        w = edges_w_col,
    )
    emin = ChordUtils.load_edges_parquet_agg(
        pq_min,
        keep_nodes;
        src = :src,
        dst = :dst,
        w = edges_w_col,
    )

    emax = ChordUtils.keep_top_edges(emax, keep_top_edges)
    emin = ChordUtils.keep_top_edges(emin, keep_top_edges)

    CSV.write(p_edges_max, emax)
    CSV.write(p_edges_min, emin)

    fig_max = ChordPlotUtils.plot_chord(
        emax,
        order_max,
        chord_cfg;
        title = "$(title_prefix) MAX year=$(y_max)",
    )
    fig_min = ChordPlotUtils.plot_chord(
        emin,
        order_min,
        chord_cfg;
        title = "$(title_prefix) MIN year=$(y_min)",
    )

    _save_both(p_svg_max, p_png_max, fig_max)
    _save_both(p_svg_min, p_png_min, fig_min)

    meta = Dict(
        "timestamp_utc" => string(MetaUtils.now_utc()),
        "pipeline" => "netsci2026/06_chord_extrema_years",
        "params" => Dict(
            "tag" => tag,
            "output_root" => output_root,
            "config" => config_path,
            "keep_top_edges" => keep_top_edges,
            "edges_weight_col" => String(edges_w_col),
            "order_desc" => order_desc,
        ),
        "inputs" => Dict(
            "step02_edges_dir" => step02_edges_dir,
            "step02_csv" => step02_csv_path,
            "keep_nodes_csv" => step03_keep_nodes_path,
            "extrema_csv" => step05_extrema_path,
        ),
        "outputs" => Dict(
            "edges_max_csv" => p_edges_max,
            "edges_min_csv" => p_edges_min,
            "svg_max" => p_svg_max,
            "svg_min" => p_svg_min,
            "png_max" => p_png_max,
            "png_min" => p_png_min,
        ),
        "hashes" => Dict(
            "config_sha256" => MetaUtils.hash_file_or_empty(config_path),
            "edges_max_sha256" => MetaUtils.hash_file_or_empty(p_edges_max),
            "edges_min_sha256" => MetaUtils.hash_file_or_empty(p_edges_min),
        ),
    )

    MetaUtils.write_json(p_meta, meta)
    println(out_dir)
end

main()
