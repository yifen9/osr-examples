module ChordUtils

using DataFrames
using DuckDB
using DBInterface

function load_edges_parquet_agg(
    parquet_path::AbstractString,
    keep_nodes::Vector{String};
    src::Symbol = :src,
    dst::Symbol = :dst,
    w::Symbol = :w,
)
    isfile(parquet_path) || error("parquet not found: $(parquet_path)")
    isempty(keep_nodes) && error("keep_nodes is empty")

    con = DBInterface.connect(DuckDB.DB, ":memory:")
    DBInterface.execute(
        con,
        "CREATE OR REPLACE TEMP VIEW edge AS SELECT * FROM read_parquet('$(parquet_path)');",
    )

    keep_list = join(["'$(replace(s, "'" => "''"))'" for s in keep_nodes], ",")
    sql =
        "SELECT " *
        "  CAST($(String(src)) AS VARCHAR) AS src, " *
        "  CAST($(String(dst)) AS VARCHAR) AS dst, " *
        "  SUM(CAST($(String(w)) AS DOUBLE)) AS w " *
        "FROM edge " *
        "WHERE $(String(src)) IS NOT NULL AND $(String(dst)) IS NOT NULL AND $(String(w)) IS NOT NULL " *
        "  AND CAST($(String(src)) AS VARCHAR) IN ($(keep_list)) " *
        "  AND CAST($(String(dst)) AS VARCHAR) IN ($(keep_list)) " *
        "GROUP BY 1,2;"

    df = DataFrame(DBInterface.execute(con, sql))
    DBInterface.close!(con)

    hasproperty(df, :src) || error("missing src")
    hasproperty(df, :dst) || error("missing dst")
    hasproperty(df, :w) || error("missing w")

    df[!, :src] = String.(df.src)
    df[!, :dst] = String.(df.dst)
    df[!, :w] = Float64.(df.w)
    return df
end

function keep_top_edges(df::DataFrame, k::Int)
    k < 0 && error("keep_top_edges must be >= 0")
    k == 0 && return df
    n = nrow(df)
    n == 0 && return df
    ord = sortperm(df.w; rev = true)
    take_n = min(k, n)
    return df[ord[1:take_n], :]
end

function node_weights_undirected(df::DataFrame)
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

end
