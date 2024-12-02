# Project Documentation

## Overview
This project is designed to store the experiment metrics.

## Usage
To use the project, follow these steps:
1. Create a postgresql database.
2. Use the provided SQL script to create the schema.
3. <python part>

## Hera workflow
    
```shell
# set the environment variables
export POSTGRES_USER="<username>" 
export POSTGRES_PASSWORD="<password>"

# print the help
python experiment.py --help

# execute the experiment
python experiment.py --versions 1 10 100 1000 --products 5 20 80 350 --steps 1 5 10 50
```

```mermaid
flowchart TD
%% Nodes
    A("<a rel="noopener" href="https://github.com/argoproj-labs/hera" target="_blank">Hera workflow</a>")
    B("<a rel="noopener" href="https://github.com/argoproj/argo-workflows" target="_blank">Argo workflows Server</a>")
    C("Argo workflows Controller")
    D((iterator))
    subgraph Experiment[<a rel="noopener" href="https://github.com/VCityTeam/ConVer-G" target="_blank">ConVer-G</a>]
        E(<a rel="noopener" href="https://hub.docker.com/r/vcity/quads-loader" target="_blank">Quads Loader</a>)
        I(<a rel="noopener" href="https://hub.docker.com/r/vcity/quads-query" target="_blank">Quads Query</a>)
        
        F(<a rel="noopener" href="https://github.com/VCityTeam/BSBM" target="_blank">Generate dataset</a>)
        subgraph Transform[<a rel="noopener" href="https://hub.docker.com/r/vcity/quads-creator" target="_blank">Transform dataset</a>]
            H1(Relational transformation)
            H2(Theoretical transformation)
        end
        G(<a rel="noopener" href="https://hub.docker.com/r/vcity/blazegraph-cors" target="_blank">Blazegraph</a>)

        J(Query backends)
    end

%% Edge connections between nodes
    A --> |submit| B --> C --> D
    D --> |starts with params| E & G & F & I
    D --> |launches queries| J
    F --> H1 & H2 
    H1 --> |Sends dataset| E
    H2 --> |Sends dataset| G
    J --> |Sends query| G & I
```

## Contributing
If you would like to contribute to this project, please follow these guidelines:
1. Fork the repository.
2. Create a new branch:
    ```sh
    git checkout -b [branch name]
    ```
3. Make your changes and commit them:
    ```sh
    git commit -m "[commit message]"
    ```
4. Push to the branch:
    ```sh
    git push origin [branch name]
    ```
5. Create a pull request.

## License
This project is licensed under the GNU Lesser General Public License v2.1. See the [LICENSE](LICENSE) file for more details.
