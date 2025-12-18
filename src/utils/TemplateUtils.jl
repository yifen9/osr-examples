module TemplateUtils

function render(s::AbstractString, vars::Dict{String,Any})
    out = String(s)
    for (k, v) in vars
        out = replace(out, "{{$(k)}}" => string(v))
    end
    return out
end

end
