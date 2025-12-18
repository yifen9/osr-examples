module PipelineUtils

using Base.Threads

function parse_pipeline_args(
    args::Vector{String};
    default_output::AbstractString = joinpath("out", "netsci2026"),
    default_config::Union{Nothing,AbstractString} = nothing,
    default_memory::AbstractString = "16GB",
    default_threads::Int = Threads.nthreads(),
)
    input_root = nothing
    output_root = default_output
    tag = nothing
    force = false
    config = default_config
    memory = default_memory
    threads = default_threads

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
    tag = tag === nothing ? basename(input_root) : String(tag)
    config = config === nothing ? nothing : abspath(String(config))

    return input_root, output_root, tag, force, config, memory, threads
end

end
