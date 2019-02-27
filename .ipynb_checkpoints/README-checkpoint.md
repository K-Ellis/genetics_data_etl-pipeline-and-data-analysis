# genetics_data_etl-pipeline-and-data-analysis

Interactive Analysis Dashboard:
https://public.tableau.com/profile/kieron4186#!/vizhome/GeneticsDataset/GeneticsData

Directions to run etl.py from docker and export PostgreSQL database (from Windows 10 home OS):
1. install docker and docker-compose
2. open terminal
3. cd to genetics_data_etl-pipeline-and-data-analysis/docker/
4. type: docker-compose up --build
5. type: docker cp docker_db_1:/var/lib/postgresql/data/. db_data

