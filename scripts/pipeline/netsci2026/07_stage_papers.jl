using OSRExamples
using OSRExamples.MetaUtils

function parse_args(args::Vector{String})
    output_root = joinpath("out", "netsci2026")
    papers_root = joinpath("papers", "netsci2026")
    tag = nothing
    force = false

    i = 1
    while i <= length(args)
        a = args[i]
        if a == "--output-root"
            i += 1
            i <= length(args) || error("missing value for --output-root")
            output_root = args[i]
        elseif a == "--papers-root"
            i += 1
            i <= length(args) || error("missing value for --papers-root")
            papers_root = args[i]
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
    papers_root = abspath(String(papers_root))
    tag = String(tag)

    return output_root, papers_root, tag, force
end

function _list_files(root::String)
    out = Vector{Dict{String,Any}}()
    for (r, _, fs) in walkdir(root)
        for f in fs
            p = joinpath(r, f)
            rel = relpath(p, root)
            push!(out, Dict("path" => rel, "bytes" => Int(filesize(p))))
        end
    end
    sort!(out, by = x -> x["path"])
    return out
end

function copytree(src::AbstractString, dst::AbstractString; force::Bool = false)
    src = String(src)
    dst = String(dst)
    isdir(src) || error("copytree: src is not a directory: $src")

    if ispath(dst)
        force || error("copytree: dst exists: $dst (use --force)")
        rm(dst; force = true, recursive = true)
    end
    mkpath(dst)

    for (root, dirs, files) in walkdir(src)
        rel = relpath(root, src)
        out_root = rel == "." ? dst : joinpath(dst, rel)
        mkpath(out_root)
        for d in dirs
            mkpath(joinpath(out_root, d))
        end
        for f in files
            s = joinpath(root, f)
            t = joinpath(out_root, f)
            cp(s, t; force = true)
        end
    end
    return dst
end

function main()
    output_root, papers_root, tag, force = parse_args(ARGS)

    src_dir = joinpath(output_root, tag)
    isdir(src_dir) || error("missing source dir: $src_dir")

    out_root = joinpath(papers_root, "out")
    mkpath(out_root)

    dst_dir = joinpath(out_root, tag)

    if isdir(dst_dir)
        force || error("dest exists: $dst_dir (use --force)")
        rm(dst_dir; recursive = true, force = true)
    end

    copytree(src_dir, dst_dir; force = force)

    meta_dir = joinpath(papers_root, "meta", tag)
    mkpath(meta_dir)

    meta_path = joinpath(meta_dir, "07_stage_papers.json")

    files = _list_files(dst_dir)
    total_bytes = 0
    for x in files
        total_bytes += Int(x["bytes"])
    end

    meta = Dict(
        "timestamp_utc" => string(MetaUtils.now_utc()),
        "pipeline" => "netsci2026/07_stage_papers",
        "params" => Dict(
            "tag" => tag,
            "output_root" => output_root,
            "papers_root" => papers_root,
        ),
        "copy" => Dict(
            "src_dir" => src_dir,
            "dst_dir" => dst_dir,
            "files_n" => length(files),
            "total_bytes" => total_bytes,
            "files" => files,
        ),
    )

    MetaUtils.write_json(meta_path, meta)
    println(dst_dir)
end

main()
