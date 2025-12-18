module FlowUtils

using DataFrames

function country_flows(
    edges::DataFrame;
    src::Symbol = :src,
    dst::Symbol = :dst,
    w::Symbol = :w,
    key::Symbol = :country_alpha2,
)
    hasproperty(edges, src) || error("missing column: $src")
    hasproperty(edges, dst) || error("missing column: $dst")
    hasproperty(edges, w) || error("missing column: $w")

    df = select(edges, src => :src, dst => :dst, w => :w)
    df = dropmissing(df, [:src, :dst, :w])
    df[!, :w] = Float64.(df.w)

    cross = df[df.src .!= df.dst, :]
    self = df[df.src .== df.dst, :]

    out = combine(groupby(cross, :src), :w => sum => :flow_out)
    inn = combine(groupby(cross, :dst), :w => sum => :flow_in)
    sel = combine(groupby(self, :src), :w => sum => :flow_self)

    rename!(out, :src => key)
    rename!(inn, :dst => key)
    rename!(sel, :src => key)

    res = outerjoin(out, inn, sel; on = key)

    res[!, :flow_out] = coalesce.(res.flow_out, 0.0)
    res[!, :flow_in] = coalesce.(res.flow_in, 0.0)
    res[!, :flow_self] = coalesce.(res.flow_self, 0.0)

    res[!, :flow_total] = res.flow_out .+ res.flow_in .+ res.flow_self

    return res
end

end
