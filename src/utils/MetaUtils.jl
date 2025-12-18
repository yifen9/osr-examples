module MetaUtils

using Dates
using SHA
using JSON3

sha256_hex(s::AbstractString) = bytes2hex(sha256(s))

read_text(path::AbstractString) = read(String(path), String)

function read_text_or_empty(path::AbstractString)
    return isfile(path) ? read_text(path) : ""
end

read_json(path::AbstractString) = JSON3.read(read_text(path))

function write_json(path::AbstractString, x)
    open(path, "w") do io
        JSON3.write(io, x)
    end
    return path
end

function hash_file_or_empty(path::AbstractString)
    return sha256_hex(read_text_or_empty(path))
end

function hash_tree(root::AbstractString; exts = [".jl"])
    root = String(root)
    isdir(root) || return sha256_hex("")
    files = String[]
    for (r, _, fs) in walkdir(root)
        for f in fs
            any(endswith(f, e) for e in exts) || continue
            push!(files, joinpath(r, f))
        end
    end
    sort!(files)
    buf = IOBuffer()
    for f in files
        rel = relpath(f, root)
        write(buf, rel)
        write(buf, '\0')
        write(buf, read(f, String))
        write(buf, '\0')
    end
    return sha256_hex(String(take!(buf)))
end

now_utc() = Dates.now(Dates.UTC)

end
