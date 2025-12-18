using SHA
using JSON3
using OSRExamples
using OSRExamples.MetaUtils

function parse_args(args::Vector{String})
    input_root = nothing
    output_root = joinpath("out", "netsci2026")
    tag = nothing
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
    tag = tag === nothing ? basename(input_root) : String(tag)

    return input_root, output_root, tag, force
end

function main()
    input_root, output_root, tag, force = parse_args(ARGS)

    upstream_path = joinpath(input_root, "_meta.json")
    isfile(upstream_path) || error("upstream _meta.json not found: $upstream_path")

    out_dir = joinpath(output_root, tag)
    mkpath(out_dir)

    out_meta_path = joinpath(out_dir, "_meta.json")
    if isfile(out_meta_path) && !force
        error("meta already exists: $out_meta_path (use --force to overwrite)")
    end

    repo_root = normpath(joinpath(@__DIR__, "..", "..", ".."))
    manifest_path = joinpath(repo_root, "Manifest.toml")
    src_root = joinpath(repo_root, "src")
    pipeline_root = joinpath(repo_root, "scripts", "pipeline", "netsci2026")

    upstream_str = read(upstream_path, String)
    upstream_obj = JSON3.read(upstream_str)

    meta = Dict(
        "timestamp_utc" => string(MetaUtils.now_utc()),
        "pipeline" => "netsci2026/00_meta",
        "params" => Dict(
            "tag" => tag,
            "input_root" => input_root,
            "output_root" => output_root,
        ),
        "upstream" =>
            Dict("product_meta_path" => upstream_path, "product_meta" => upstream_obj),
        "hashes" => Dict(
            "product_meta_sha256" => MetaUtils.sha256_hex(upstream_str),
            "manifest_sha256" => MetaUtils.hash_file_or_empty(manifest_path),
            "src_sha256" => MetaUtils.hash_tree(src_root; exts = [".jl"]),
            "pipeline_dir_sha256" => MetaUtils.hash_tree(pipeline_root; exts = [".jl"]),
        ),
    )

    MetaUtils.write_json(out_meta_path, meta)
    println(out_dir)
end

main()
