module RunConfigUtils

function write_query_yaml(path::AbstractString, edge::AbstractString, sql::AbstractString)
    open(path, "w") do io
        write(io, "edge: ", edge, "\n")
        write(io, "sql: |\n")
        for line in split(String(sql), "\n"; keepempty = true)
            write(io, "  ", line, "\n")
        end
    end
    return String(path)
end

function write_run_yaml(
    path::AbstractString,
    edge::AbstractString,
    sql::AbstractString,
    compute::Dict,
)
    open(path, "w") do io
        write(io, "edge: ", edge, "\n")
        write(io, "sql: |\n")
        for line in split(String(sql), "\n"; keepempty = true)
            write(io, "  ", line, "\n")
        end
        write(io, "\ncompute:\n")
        write(io, "  alg: ", string(compute["alg"]), "\n")
        if haskey(compute, "lambda")
            write(io, "  lambda: ", string(compute["lambda"]), "\n")
        elseif haskey(compute, "λ")
            write(io, "  lambda: ", string(compute["λ"]), "\n")
        end
        if haskey(compute, "method")
            write(io, "  method: ", string(compute["method"]), "\n")
        end
        write(io, "  src: ", string(compute["src"]), "\n")
        write(io, "  dst: ", string(compute["dst"]), "\n")
        write(io, "  w: ", string(compute["w"]), "\n")
    end
    return String(path)
end

end
