using CSV
using DataFrames
using YAML
using CairoMakie
using Printf
using OSRExamples
using OSRExamples.MetaUtils
using OSRExamples.StatsUtils

function parse_args(args::Vector{String})
    output_root = joinpath("out", "netsci2026")
    tag = nothing
    config =
        joinpath("papers", "netsci2026", "config", "05_country_springrank_dispersion.yaml")
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

function _opt_float(x)
    x === nothing && return nothing
    return Float64(x)
end

function main()
    output_root, tag, config_path, force = parse_args(ARGS)
    cfg = YAML.load_file(config_path)

    up = cfg["upstream"]
    step02_dir = String(up["step02_dir"])
    in02_name = String(up["input02_csv"])
    step03_dir = String(up["step03_dir"])
    keep_years_name = String(up["keep_years_csv"])

    cols = cfg["columns"]
    c_year = _as_sym(cols["year"])
    c_country = _as_sym(cols["country"])
    c_score = _as_sym(cols["score"])
    c_w = _as_sym(cols["weight"])
    c_ws = _as_sym(cols["window_start"])
    c_we = _as_sym(cols["window_end_excl"])
    c_ww = _as_sym(cols["window_width"])
    c_wm = _as_sym(cols["window_mode"])

    out_cfg = cfg["output"]
    step05_dir = String(out_cfg["step05_dir"])
    out_csv_all_name = String(out_cfg["csv_all"])
    out_svg_kept_name = String(out_cfg["svg_kept"])

    plot_cfg = cfg["plot"]
    title = String(plot_cfg["title"])
    figw = Int(plot_cfg["width"])
    figh = Int(plot_cfg["height"])
    xlab = String(plot_cfg["x_label"])
    ylab = String(plot_cfg["y_label"])
    x_tick_step = Int(plot_cfg["x_tick_step"])
    y_tick_step = Float64(plot_cfg["y_tick_step"])
    y_digits = Int(plot_cfg["y_format_digits"])
    ylims_cfg = plot_cfg["y_limits"]
    ylo_cfg = _opt_float(ylims_cfg[1])
    yhi_cfg = _opt_float(ylims_cfg[2])

    base_dir = joinpath(output_root, tag)
    in02 = joinpath(base_dir, step02_dir, in02_name)
    keep_years_csv = joinpath(base_dir, step03_dir, keep_years_name)

    isfile(in02) || error("missing input: $in02")
    isfile(keep_years_csv) || error("missing input: $keep_years_csv")

    out_dir = joinpath(base_dir, step05_dir)
    mkpath(out_dir)

    out_csv_all = joinpath(out_dir, out_csv_all_name)
    out_svg_kept = joinpath(out_dir, out_svg_kept_name)
    out_meta = joinpath(out_dir, "_meta.json")

    if (isfile(out_csv_all) || isfile(out_svg_kept)) && !force
        error("outputs exist in: $out_dir (use --force to overwrite)")
    end

    df = DataFrame(CSV.File(in02))
    for c in (c_year, c_country, c_score, c_w, c_ws, c_we, c_ww, c_wm)
        hasproperty(df, c) || error("missing column: $(c)")
    end

    replace!(df[!, c_w], missing => 0)
    replace!(df[!, c_score], missing => 0)

    df[!, c_year] = Int.(df[!, c_year])
    df[!, c_w] = Float64.(df[!, c_w])
    df[!, c_score] = Float64.(df[!, c_score])
    df[!, c_country] = String.(df[!, c_country])
    df[!, c_wm] = String.(df[!, c_wm])

    g = groupby(df, c_year)

    rows = DataFrame(
        year_center = Int[],
        window_start_year = Int[],
        window_end_year_excl = Int[],
        window_width = Int[],
        window_mode = String[],
        countries_n = Int[],
        weight_sum = Float64[],
        score_mean_weighted = Float64[],
        score_std_weighted = Float64[],
    )

    for sub in g
        y = Int(first(sub[!, c_year]))
        ws = Int(first(sub[!, c_ws]))
        we = Int(first(sub[!, c_we]))
        ww = Int(first(sub[!, c_ww]))
        wm = String(first(sub[!, c_wm]))

        wv = sub[!, c_w]
        sv = sub[!, c_score]

        ncty = length(unique(sub[!, c_country]))
        wsum = sum(wv)
        μw = StatsUtils.weighted_mean(sv, wv)
        sdw = StatsUtils.weighted_std_nist(sv, wv)

        push!(
            rows,
            (
                year_center = y,
                window_start_year = ws,
                window_end_year_excl = we,
                window_width = ww,
                window_mode = wm,
                countries_n = ncty,
                weight_sum = Float64(wsum),
                score_mean_weighted = Float64(μw),
                score_std_weighted = Float64(sdw),
            ),
        )
    end

    sort!(rows, :year_center)
    CSV.write(out_csv_all, rows)

    keepy_df = DataFrame(CSV.File(keep_years_csv))
    hasproperty(keepy_df, :year_center) ||
        error("keep_years_02.csv missing column: year_center")
    keep_set = Set(Int.(keepy_df.year_center))

    rows_kept = rows[in.(rows.year_center, Ref(keep_set)), :]
    years = rows_kept.year_center
    ys = rows_kept.score_std_weighted

    fig = Figure(size = (figw, figh))
    ax = Axis(fig[1, 1], title = title, xlabel = xlab, ylabel = ylab)

    if !isempty(years)
        ax.xticks = collect(minimum(years):x_tick_step:maximum(years))
        ax.xtickformat = xs -> string.(Int.(xs))
    end

    ylo = ylo_cfg
    yhi = yhi_cfg
    if ylo === nothing || yhi === nothing
        if isempty(ys)
            ylo = ylo === nothing ? 0.0 : ylo
            yhi = yhi === nothing ? 1.0 : yhi
        else
            mn = minimum(ys)
            mx = maximum(ys)
            pad = max(0.05 * (mx - mn), y_tick_step)
            ylo = ylo === nothing ? (floor((mn - pad) / y_tick_step) * y_tick_step) : ylo
            yhi = yhi === nothing ? (ceil((mx + pad) / y_tick_step) * y_tick_step) : yhi
        end
    end

    ylims!(ax, ylo, yhi)
    yt = collect(ylo:y_tick_step:yhi)
    ax.yticks = (yt, [@sprintf("%.*f", y_digits, v) for v in yt])

    lines!(ax, years, ys)
    scatter!(ax, years, ys)

    save(out_svg_kept, fig)

    meta = Dict(
        "timestamp_utc" => string(MetaUtils.now_utc()),
        "pipeline" => "netsci2026/05_country_springrank_dispersion",
        "params" => Dict(
            "tag" => tag,
            "output_root" => output_root,
            "config" => config_path,
            "weighted_std" => "nist_dataplot_eq_2_22",
        ),
        "inputs" => Dict("02_csv_all" => in02, "03_keep_years_csv" => keep_years_csv),
        "hashes" => Dict(
            "config_sha256" => MetaUtils.hash_file_or_empty(config_path),
            "in02_sha256" => MetaUtils.hash_file_or_empty(in02),
            "keep_years_sha256" => MetaUtils.hash_file_or_empty(keep_years_csv),
            "out_csv_all_sha256" => MetaUtils.hash_file_or_empty(out_csv_all),
            "out_svg_kept_sha256" => MetaUtils.hash_file_or_empty(out_svg_kept),
        ),
    )

    MetaUtils.write_json(out_meta, meta)
    println(out_dir)
end

main()
