add PROJECT NAME:
    julia --project={{PROJECT}} -e 'using Pkg; Pkg.add("{{NAME}}")'

rm PROJECT NAME:
    julia --project={{PROJECT}} -e 'using Pkg; Pkg.rm("{{NAME}}")'

up:
    julia --project=. -e 'using Pkg; Pkg.update()'
    julia --project=dev -e 'using Pkg; Pkg.update()'

init:
    julia --project=. -e 'using Pkg; Pkg.instantiate()'
    julia --project=dev -e 'using Pkg; Pkg.instantiate()'
    julia --project=dev -e 'using Pkg; Pkg.develop(path=".")'

resolve:
    julia --project=. -e 'using Pkg; Pkg.resolve()'
    julia --project=dev -e 'using Pkg; Pkg.resolve()'

fmt:
    julia --project=dev -e 'using Pkg; Pkg.instantiate(); using JuliaFormatter; format("src")'
