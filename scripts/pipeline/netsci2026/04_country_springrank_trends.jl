using CSV
using DataFrames
using Statistics
using YAML
using CairoMakie
using Printf
using OSRExamples
using OSRExamples.MetaUtils

function parse_args(args::Vector{String})
    output_root = joinpath("out", "netsci2026")
    tag = nothing
    config = joinpath("papers", "netsci2026", "config", "04_country_springrank_trends.yaml")
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

function _palette(name::String, n::Int)
    if name == "wong"
        base = Makie.wong_colors()
        k = length(base)
        return [base[(i-1)%k+1] for i = 1:n]
    else
        error("unknown palette: $name")
    end
end

function _opt_float(x)
    x === nothing && return nothing
    return Float64(x)
end

function main()
    output_root, tag, config_path, force = parse_args(ARGS)

    raw = YAML.load_file(config_path)
    up = raw["upstream"]
    step03_dir = String(up["step03_dir"])
    step02_file = String(up["step02_file"])

    fr = raw["filter"]
    flow_col = Symbol(String(fr["flow_col"]))
    thr = String(fr["threshold"])
    thr == "above_mean" || error("filter.threshold must be above_mean")

    pr = raw["plot"]
    out_name = String(pr["output_name"])
    w = Int(pr["width"])
    h = Int(pr["height"])
    title = String(pr["title"])
    xlabel = String(pr["xlabel"])
    ylabel = String(pr["ylabel"])
    lw = Float64(pr["linewidth"])
    alpha = Float64(pr["alpha"])
    pal_name = String(pr["palette"])

    axcfg = pr["axis"]
    xcfg = axcfg["x"]
    ycfg = axcfg["y"]

    xticks_mode = String(xcfg["ticks"])
    xticks_step = Int(xcfg["tick_step"])
    xpad_right = Float64(xcfg["pad_right"])
    xlims_cfg = xcfg["limits"]

    yticks_mode = String(ycfg["ticks"])
    yticks_step = Float64(ycfg["tick_step"])
    ylims_cfg = ycfg["limits"]
    ydigits = Int(ycfg["format_digits"])

    lg = pr["legend"]
    legend_enabled = Bool(lg["enabled"])
    legend_title = String(lg["title"])
    legend_ncol = Int(lg["ncol"])
    legend_frame = Bool(lg["framevisible"])

    base_dir = joinpath(output_root, tag)
    in_dir = joinpath(base_dir, step03_dir)
    in02 = joinpath(in_dir, step02_file)
    isfile(in02) || error("missing input: $in02")

    out_dir = joinpath(base_dir, "04_country_springrank_trends")
    mkpath(out_dir)

    out_svg = joinpath(out_dir, out_name)
    out_csv = joinpath(out_dir, "springrank_trends_filtered.csv")
    out_keep = joinpath(out_dir, "keep_countries_04.csv")
    out_meta = joinpath(out_dir, "_meta.json")

    if (isfile(out_svg) || isfile(out_csv)) && !force
        error("outputs exist in: $out_dir (use --force to overwrite)")
    end

    df = DataFrame(CSV.File(in02))
    hasproperty(df, :country_alpha2) || error("missing column: country_alpha2")
    hasproperty(df, :year_center) || error("missing column: year_center")
    hasproperty(df, :springrank_score) || error("missing column: springrank_score")
    hasproperty(df, flow_col) || error("missing column: $(String(flow_col))")

    df[!, :country_alpha2] = String.(df.country_alpha2)
    df[!, :year_center] = Int.(df.year_center)
    df[!, :springrank_score] = Float64.(df.springrank_score)
    df[!, flow_col] = Float64.(df[!, flow_col])

    byc = combine(groupby(df, :country_alpha2), flow_col => sum => :flow_total_sum)
    m = mean(byc.flow_total_sum)
    keep = byc.country_alpha2[byc.flow_total_sum .> m]
    keep_set = Set(keep)

    df_f = df[in.(df.country_alpha2, Ref(keep_set)), :]
    sort!(df_f, [:country_alpha2, :year_center])

    CSV.write(out_csv, df_f)
    CSV.write(out_keep, DataFrame(country_alpha2 = sort(collect(keep_set))))

    g = groupby(df_f, :country_alpha2)
    last_score = Dict{String,Float64}()
    last_year = Dict{String,Int}()
    for sub in g
        sort!(sub, :year_center)
        c = sub.country_alpha2[end]
        last_score[c] = sub.springrank_score[end]
        last_year[c] = sub.year_center[end]
    end

    countries = sort(collect(keys(last_score)); by = c -> (-last_score[c], c))

    cols = _palette(pal_name, length(countries))
    cmap = Dict(countries[i] => cols[i] for i in eachindex(countries))

    x_min_data = minimum(df_f.year_center)
    x_max_data = maximum(df_f.year_center)

    xlim_min_cfg = _opt_float(xlims_cfg[1])
    xlim_max_cfg = _opt_float(xlims_cfg[2])
    xlim_min = xlim_min_cfg === nothing ? Float64(x_min_data) : xlim_min_cfg
    xlim_max = xlim_max_cfg === nothing ? Float64(x_max_data) + xpad_right : xlim_max_cfg

    ylim_min_cfg = _opt_float(ylims_cfg[1])
    ylim_max_cfg = _opt_float(ylims_cfg[2])

    fig = Figure(size = (w, h))
    ax = Axis(fig[1, 1], title = title, xlabel = xlabel, ylabel = ylabel)

    xlims!(ax, xlim_min, xlim_max)
    if ylim_min_cfg !== nothing && ylim_max_cfg !== nothing
        ylims!(ax, ylim_min_cfg, ylim_max_cfg)
    end

    if xticks_mode == "each_year"
        ax.xticks = collect(x_min_data:xticks_step:x_max_data)
        ax.xtickformat = xs -> string.(Int.(xs))
    elseif xticks_mode == "auto"
    else
        error("axis.x.ticks must be each_year or auto")
    end

    if yticks_mode == "auto"
    elseif yticks_mode == "step"
        if ylim_min_cfg === nothing || ylim_max_cfg === nothing
            mn = minimum(df_f.springrank_score)
            mx = maximum(df_f.springrank_score)
            pad = max(0.05 * (mx - mn), yticks_step)
            y0 = floor((mn - pad) / yticks_step) * yticks_step
            y1 = ceil((mx + pad) / yticks_step) * yticks_step
            ylims!(ax, y0, y1)
            ylim_min_cfg = y0
            ylim_max_cfg = y1
        end
        yt = collect(ylim_min_cfg:yticks_step:ylim_max_cfg)
        ax.yticks = (yt, [@sprintf("%.*f", ydigits, v) for v in yt])
    else
        error("axis.y.ticks must be auto or step")
    end

    labels = String[]
    elements = Any[]

    for c in countries
        sub = df_f[df_f.country_alpha2 .== c, :]
        xs = Float64.(sub.year_center)
        ys = sub.springrank_score
        lines!(
            ax,
            xs,
            ys;
            color = cmap[c],
            linewidth = lw,
            transparency = true,
            alpha = alpha,
        )
        push!(labels, uppercase(c))
        push!(
            elements,
            PolyElement(color = cmap[c], strokecolor = cmap[c], strokewidth = 1.5),
        )
    end

    if legend_enabled
        Legend(
            fig[1, 2],
            elements,
            labels,
            legend_title;
            nbanks = legend_ncol,
            framevisible = legend_frame,
            labelcolor = :black,
        )
        colsize!(fig.layout, 2, Auto(0.25))
    end

    save(out_svg, fig)

    in03_meta = joinpath(in_dir, "_meta.json")
    upstream = isfile(in03_meta) ? MetaUtils.read_json(in03_meta) : Dict()

    meta = Dict(
        "timestamp_utc" => string(MetaUtils.now_utc()),
        "pipeline" => "netsci2026/04_country_springrank_trends",
        "params" =>
            Dict("tag" => tag, "output_root" => output_root, "config" => config_path),
        "inputs" => Dict("02_window_filtered_csv" => in02, "03_meta" => in03_meta),
        "upstream" => Dict("03_meta" => upstream),
        "hashes" => Dict(
            "config_sha256" => MetaUtils.hash_file_or_empty(config_path),
            "in02_sha256" => MetaUtils.hash_file_or_empty(in02),
            "out_svg_sha256" => MetaUtils.hash_file_or_empty(out_svg),
            "out_csv_sha256" => MetaUtils.hash_file_or_empty(out_csv),
        ),
        "stats" => Dict(
            "countries_total" => length(unique(df.country_alpha2)),
            "countries_kept" => length(keep_set),
            "mean_flow_total_sum" => m,
        ),
    )

    MetaUtils.write_json(out_meta, meta)
    println(out_dir)
end

main()
