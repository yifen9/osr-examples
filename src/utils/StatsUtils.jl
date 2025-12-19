module StatsUtils

function weighted_mean(x::AbstractVector, w::AbstractVector)
    length(x) == length(w) || error("x and w length mismatch")
    s = 0.0
    sw = 0.0
    @inbounds for i in eachindex(x, w)
        wi = Float64(w[i])
        xi = Float64(x[i])
        if !isfinite(wi) || wi <= 0
            continue
        end
        s += wi * xi
        sw += wi
    end
    sw == 0.0 && return 0.0
    return s / sw
end

function weighted_std_nist(x::AbstractVector, w::AbstractVector)
    length(x) == length(w) || error("x and w length mismatch")
    μ = weighted_mean(x, w)
    sw = 0.0
    ss = 0.0
    npos = 0
    @inbounds for i in eachindex(x, w)
        wi = Float64(w[i])
        xi = Float64(x[i])
        if !isfinite(wi) || wi <= 0
            continue
        end
        npos += 1
        sw += wi
        d = xi - μ
        ss += wi * d * d
    end
    (npos <= 1 || sw == 0.0) && return 0.0
    return sqrt((npos * ss) / ((npos - 1) * sw))
end

end
