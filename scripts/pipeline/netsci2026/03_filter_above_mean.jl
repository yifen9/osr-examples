using CSV
using DataFrames
using OSRExamples
using OSRExamples.MetaUtils
using Statistics

function parse_args(args::Vector{String})
    output_root = joinpath("out", "netsci2026")
    tag = nothing
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

    return output_root, tag, force
end

function main()
    output_root, tag, force = parse_args(ARGS)

    base_dir = joinpath(output_root, tag)
    in01 = joinpath(base_dir, "01_country_springrank", "springrank.csv")
    in02 = joinpath(base_dir, "02_country_springrank_window", "springrank_windowed.csv")

    isfile(in01) || error("missing input: $in01")
    isfile(in02) || error("missing input: $in02")

    out_dir = joinpath(base_dir, "03_filter_above_mean")
    mkpath(out_dir)

    out01 = joinpath(out_dir, "01_country_filtered.csv")
    out02 = joinpath(out_dir, "02_window_filtered.csv")
    out_keep01 = joinpath(out_dir, "keep_countries_01.csv")
    out_keep02 = joinpath(out_dir, "keep_countries_02.csv")
    out_keep_y = joinpath(out_dir, "keep_years_02.csv")
    out_meta = joinpath(out_dir, "_meta.json")

    if (isfile(out01) || isfile(out02)) && !force
        error("outputs exist in: $out_dir (use --force to overwrite)")
    end

    df1 = DataFrame(CSV.File(in01))
    df2 = DataFrame(CSV.File(in02))

    m1 = mean(df1.flow_total)
    keep1 = df1.country_alpha2[df1.flow_total .> m1]
    keep1_set = Set(keep1)
    df1_f = df1[in.(df1.country_alpha2, Ref(keep1_set)), :]

    byc = combine(groupby(df2, :country_alpha2), :flow_total => sum => :flow_total_sum)
    m2c = mean(byc.flow_total_sum)
    keep2 = byc.country_alpha2[byc.flow_total_sum .> m2c]
    keep2_set = Set(keep2)

    byy = combine(groupby(df2, :year_center), :flow_total => sum => :year_flow_total)
    m2y = mean(byy.year_flow_total)
    keepy = byy.year_center[byy.year_flow_total .> m2y]
    keepy_set = Set(keepy)

    mask = in.(df2.country_alpha2, Ref(keep2_set)) .&& in.(df2.year_center, Ref(keepy_set))
    df2_f = df2[mask, :]

    CSV.write(out01, df1_f)
    CSV.write(out02, df2_f)
    CSV.write(out_keep01, DataFrame(country_alpha2 = collect(keep1_set)))
    CSV.write(out_keep02, DataFrame(country_alpha2 = collect(keep2_set)))
    CSV.write(out_keep_y, DataFrame(year_center = sort(collect(keepy_set))))

    meta = Dict(
        "timestamp_utc" => string(MetaUtils.now_utc()),
        "pipeline" => "netsci2026/03_filter_above_mean",
        "params" => Dict("tag" => tag, "output_root" => output_root),
        "inputs" => Dict("01_csv" => in01, "02_csv" => in02),
        "hashes" => Dict(
            "in01_sha256" => MetaUtils.hash_file_or_empty(in01),
            "in02_sha256" => MetaUtils.hash_file_or_empty(in02),
            "out01_sha256" => MetaUtils.hash_file_or_empty(out01),
            "out02_sha256" => MetaUtils.hash_file_or_empty(out02),
        ),
    )

    MetaUtils.write_json(out_meta, meta)
    println(out_dir)
end

main()
