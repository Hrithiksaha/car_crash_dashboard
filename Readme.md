Analysis for the casse study of Car Crashes. 

The Analysis folder consists of 2 .ipynb files which shows how the analysis were made in the initial stages. 




### Runbook
Clone the repo and follow these steps:
1. Go to the Project Directory: `$ cd Car_Crash_Case_Study`
2. On terminal, run `$ make build`. This will build the project to run via spark-submit. In this process a new folder with 
   name "dist" is created, and the code artefacts are copied into it.
3. `$ cd Dist && spark-submit --master "local[*]" --py-files src.zip --files config.yaml main.py && cd ..`