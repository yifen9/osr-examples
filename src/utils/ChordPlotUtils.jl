module ChordPlotUtils

using DataFrames
using CairoMakie

_as_sym(x) = Symbol(String(x))

function _req(d, k::String)
    haskey(d, k) || error("missing key: $(k)")
    return d[k]
end

function _hsv_to_rgb(h::Float64, s::Float64, v::Float64)
    h = mod(h, 1.0)
    i = floor(Int, h * 6)
    f = h * 6 - i
    p = v * (1 - s)
    q = v * (1 - f * s)
    t = v * (1 - (1 - f) * s)
    j = mod(i, 6)
    if j == 0
        return (v, t, p)
    elseif j == 1
        return (q, v, p)
    elseif j == 2
        return (p, v, t)
    elseif j == 3
        return (p, q, v)
    elseif j == 4
        return (t, p, v)
    else
        return (v, p, q)
    end
end

function _palette_linear(n::Int; h_start::Float64, h_end::Float64, s::Float64, v::Float64)
    n > 0 || error("n must be > 0")
    cols = NTuple{4,Float64}[]
    if n == 1
        r, g, b = _hsv_to_rgb(h_start, s, v)
        push!(cols, (r, g, b, 1.0))
        return cols
    end
    for i = 0:(n-1)
        t = i / (n - 1)
        h = h_start + (h_end - h_start) * t
        r, g, b = _hsv_to_rgb(h, s, v)
        push!(cols, (r, g, b, 1.0))
    end
    return cols
end

function _polar(r::Float64, θ::Float64)
    return Point2f(r * cos(θ), r * sin(θ))
end

function _sector_poly(r_in::Float64, r_out::Float64, θ1::Float64, θ2::Float64, n::Int)
    n >= 4 || error("arc_samples must be >= 4")
    θs = range(θ1, θ2; length = n)
    outer = [_polar(r_out, θ) for θ in θs]
    inner = [_polar(r_in, θ) for θ in reverse(θs)]
    return vcat(outer, inner)
end

function _bezier(p0::Point2f, p1::Point2f, p2::Point2f, p3::Point2f, t::Float32)
    u = 1.0f0 - t
    return (u^3) * p0 + 3.0f0 * (u^2) * t * p1 + 3.0f0 * u * (t^2) * p2 + (t^3) * p3
end

function _ribbon_poly(
    a1::Float64,
    a2::Float64,
    b1::Float64,
    b2::Float64;
    r_src::Float64,
    r_dst::Float64,
    r_ctrl::Float64,
    samples::Int,
)
    samples >= 8 || error("ribbon_samples must be >= 8")
    p0 = _polar(r_src, a1)
    p3 = _polar(r_dst, b1)
    c1 = _polar(r_ctrl, a1)
    c2 = _polar(r_ctrl, b1)

    q0 = _polar(r_src, a2)
    q3 = _polar(r_dst, b2)
    d1 = _polar(r_ctrl, a2)
    d2 = _polar(r_ctrl, b2)

    ts = range(0.0f0, 1.0f0; length = samples)
    left = [_bezier(p0, c1, c2, p3, t) for t in ts]
    right = [_bezier(q0, d1, d2, q3, t) for t in ts]

    return vcat(left, reverse(right))
end

function _node_weights(df::DataFrame)
    outw = combine(groupby(df, :src), :w => sum => :w_out)
    inw = combine(groupby(df, :dst), :w => sum => :w_in)
    rename!(outw, :src => :node)
    rename!(inw, :dst => :node)
    m = outerjoin(outw, inw; on = :node)
    m[!, :w_out] = coalesce.(m.w_out, 0.0)
    m[!, :w_in] = coalesce.(m.w_in, 0.0)
    m[!, :w_total] = m.w_out .+ m.w_in
    return m
end

function plot_chord(
    edges::DataFrame,
    node_order::Vector{String},
    cfg::Dict{Any,Any};
    title::String,
)
    n = length(node_order)
    n > 0 || error("node_order is empty")

    arc_samples = Int(_req(cfg, "arc_samples"))
    ribbon_samples = Int(_req(cfg, "ribbon_samples"))

    r_inner = Float64(_req(cfg, "ring_inner_radius"))
    r_outer = Float64(_req(cfg, "ring_outer_radius"))
    r_attach_out = Float64(_req(cfg, "ribbon_attach_radius_out"))
    r_attach_in = Float64(_req(cfg, "ribbon_attach_radius_in"))
    r_ctrl = Float64(_req(cfg, "ribbon_control_radius"))

    gap_deg = Float64(_req(cfg, "node_gap_deg"))
    start_deg = Float64(_req(cfg, "start_angle_deg"))
    clockwise = Bool(_req(cfg, "clockwise"))

    col_hs = Float64(_req(cfg, "palette_h_start"))
    col_he = Float64(_req(cfg, "palette_h_end"))
    col_s = Float64(_req(cfg, "palette_s"))
    col_v = Float64(_req(cfg, "palette_v"))

    ribbon_alpha = Float64(_req(cfg, "ribbon_alpha"))

    label_enable = Bool(_req(cfg, "label_enable"))
    label_radius = Float64(_req(cfg, "label_radius"))
    label_fontsize = Float64(_req(cfg, "label_fontsize"))
    label_pad = Float64(_req(cfg, "label_pad"))
    label_border_width = Float64(_req(cfg, "label_border_width"))
    label_corner_radius = Float64(_req(cfg, "label_corner_radius"))

    figw = Int(_req(cfg, "figure_width"))
    figh = Int(_req(cfg, "figure_height"))
    pad = Int(_req(cfg, "figure_padding"))

    df = select(edges, :src, :dst, :w)
    df = dropmissing(df, [:src, :dst, :w])
    df[!, :src] = String.(df.src)
    df[!, :dst] = String.(df.dst)
    df[!, :w] = Float64.(df.w)

    keep = Set(node_order)
    mask = in.(df.src, Ref(keep)) .&& in.(df.dst, Ref(keep)) .&& (df.w .> 0)
    df = df[mask, :]

    wtab = _node_weights(df)
    wmap = Dict{String,Tuple{Float64,Float64,Float64}}()
    for r in eachrow(wtab)
        wmap[String(r.node)] = (Float64(r.w_out), Float64(r.w_in), Float64(r.w_total))
    end
    for x in node_order
        haskey(wmap, x) || (wmap[x] = (0.0, 0.0, 0.0))
    end

    total_all = sum(wmap[x][3] for x in node_order)
    total_all > 0 || error("total weight is 0")

    gap = deg2rad(gap_deg)
    start = deg2rad(start_deg)
    dir = clockwise ? -1.0 : 1.0
    avail = 2pi - n * gap
    avail > 0 || error("node_gap_deg too large")

    cols = _palette_linear(n; h_start = col_hs, h_end = col_he, s = col_s, v = col_v)
    node_color = Dict(node_order[i] => cols[i] for i = 1:n)

    rank = Dict{String,Int}()
    for (i, x) in enumerate(node_order)
        rank[x] = i
    end

    θ_start = Dict{String,Float64}()
    θ_end = Dict{String,Float64}()

    θ = start
    for x in node_order
        frac = wmap[x][3] / total_all
        span = avail * frac
        θ_start[x] = θ
        θ_end[x] = θ + dir * span
        θ = θ_end[x] + dir * gap
    end

    out_span = Dict{String,Tuple{Float64,Float64}}()
    in_span = Dict{String,Tuple{Float64,Float64}}()

    for x in node_order
        a = θ_start[x]
        b = θ_end[x]
        tot = wmap[x][3]
        wo = wmap[x][1]
        wi = wmap[x][2]
        if tot == 0
            out_span[x] = (a, a)
            in_span[x] = (a, a)
        else
            f_out = wo / tot
            mid = a + dir * abs(b - a) * f_out
            out_span[x] = (a, mid)
            in_span[x] = (mid, b)
        end
    end

    src_groups = Dict{String,DataFrame}()
    for sub in groupby(df, :src)
        k = String(first(sub.src))
        src_groups[k] = DataFrame(sub)
    end

    dst_groups = Dict{String,DataFrame}()
    for sub in groupby(df, :dst)
        k = String(first(sub.dst))
        dst_groups[k] = DataFrame(sub)
    end

    out_pos = Dict{Tuple{String,String},Tuple{Float64,Float64}}()
    in_pos = Dict{Tuple{String,String},Tuple{Float64,Float64}}()

    for x in node_order
        a0, a1 = out_span[x]
        span = abs(a1 - a0)
        sub = get(src_groups, x, DataFrame(src = String[], dst = String[], w = Float64[]))
        if nrow(sub) > 0
            ord = sortperm([get(rank, String(z), typemax(Int)) for z in sub.dst])
            sub = sub[ord, :]
        end
        wsum = sum(sub.w)
        cur = a0
        for r in eachrow(sub)
            dst = String(r.dst)
            w = Float64(r.w)
            frac = (wsum == 0) ? 0.0 : (w / wsum)
            dθ = span * frac
            lo = cur
            hi = cur + dir * dθ
            out_pos[(x, dst)] = (lo, hi)
            cur = hi
        end
    end

    for x in node_order
        b0, b1 = in_span[x]
        span = abs(b1 - b0)
        sub = get(dst_groups, x, DataFrame(src = String[], dst = String[], w = Float64[]))
        if nrow(sub) > 0
            ord = sortperm([get(rank, String(z), typemax(Int)) for z in sub.src])
            sub = sub[ord, :]
        end
        wsum = sum(sub.w)
        cur = b0
        for r in eachrow(sub)
            src = String(r.src)
            w = Float64(r.w)
            frac = (wsum == 0) ? 0.0 : (w / wsum)
            dθ = span * frac
            lo = cur
            hi = cur + dir * dθ
            in_pos[(src, x)] = (lo, hi)
            cur = hi
        end
    end

    fig = Figure(size = (figw, figh), figure_padding = pad)
    ax = Axis(
        fig[1, 1],
        title = title,
        titlesize = Float64(_req(cfg, "title_fontsize")),
        aspect = DataAspect(),
    )
    hidespines!(ax)
    hidedecorations!(ax)

    r_lim = max(r_outer, label_radius) + 0.15 * r_outer
    xlims!(ax, -r_lim, r_lim)
    ylims!(ax, -r_lim, r_lim)

    for x in node_order
        a = θ_start[x]
        b = θ_end[x]
        col = node_color[x]
        pts = _sector_poly(r_inner, r_outer, a, b, arc_samples)
        poly!(ax, pts; color = col)
    end

    for r in eachrow(df)
        s = String(r.src)
        t = String(r.dst)
        haskey(out_pos, (s, t)) || continue
        haskey(in_pos, (s, t)) || continue

        a1, a2 = out_pos[(s, t)]
        b1, b2 = in_pos[(s, t)]

        a_lo = min(a1, a2)
        a_hi = max(a1, a2)
        b_lo = min(b1, b2)
        b_hi = max(b1, b2)

        poly_pts = _ribbon_poly(
            a_lo,
            a_hi,
            b_lo,
            b_hi;
            r_src = r_attach_out,
            r_dst = r_attach_in,
            r_ctrl = r_ctrl,
            samples = ribbon_samples,
        )

        c = node_color[s]
        fillc = (c[1], c[2], c[3], ribbon_alpha)
        poly!(ax, poly_pts; color = fillc)
    end

    if label_enable
        for x in node_order
            a = θ_start[x]
            b = θ_end[x]
            mid = (a + b) / 2
            p = _polar(label_radius, mid)
            txt = uppercase(x)
            c = node_color[x]
            borderc = (c[1], c[2], c[3], 1.0)
            textlabel!(
                ax,
                [p];
                text = [txt],
                fontsize = label_fontsize,
                text_color = :black,
                text_rotation = 0.0,
                text_align = (:center, :center),
                background_color = (0.0, 0.0, 0.0, 0.0),
                padding = label_pad,
                cornerradius = label_corner_radius,
                strokecolor = borderc,
                strokewidth = label_border_width,
            )
        end
    end

    return fig
end

end
