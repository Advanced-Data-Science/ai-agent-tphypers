[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/VPdckhXm)
#### About: 
This repo contains all necessary files to create an AI Agent that will interact with the Weather API and OpenWeatherMap API to collect data on 5 different cities in a respectful manner. It will be able to handle and adapt to issues/errors while recording what has happened. It will also assess collection quality, collect raw data, process the data, and store metadata. 

#### How to use:
##### Set up:
Make sure to have the following file setup:  
your_project_root/  
├── agent/  
│   ├── data_collection_agent.py  <-- The agent script  
│   └── config.json               <-- MANDATORY: Your configuration file (create this)  
├── data/  
│   ├── raw/                      <-- Created by script on run  
│   ├── processed/                <-- Created by script on run  
│   └── metadata/                 <-- Created by script on run  
├── logs/                         <-- Created by script on run  
└── reports/                      <-- Created by script on run  

##### Usage:
Install the requests library for python. You must also populate the config.json file with valid API keys (replace the placeholder values of 'YOUR...KEY'). Finally, you will be able to run the agent from the data_collection_agent.py file. This will collect data from the cities specified in the config.json file and create files storing the raw data, processed data, and metadata as well as a report on the quality and collection summary.
