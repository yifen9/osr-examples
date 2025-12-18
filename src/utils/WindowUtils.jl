module WindowUtils

function window_bounds_year(center::Integer, width::Integer; mode::Symbol = :center_right)
    w = Int(width)
    w > 0 || error("width must be > 0")
    c = Int(center)

    if mode == :center_right
        start = c - ((w - 1) ÷ 2)
        stop_excl = start + w
        return start, stop_excl
    elseif mode == :center_left
        start = c - (w ÷ 2)
        stop_excl = start + w
        return start, stop_excl
    elseif mode == :trailing
        start = c - w + 1
        stop_excl = c + 1
        return start, stop_excl
    elseif mode == :leading
        start = c
        stop_excl = c + w
        return start, stop_excl
    else
        error("unknown mode: $mode")
    end
end

end
