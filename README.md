# Taxi Availability Demand Forecasting

Built by Darren Chan (1006340), Isaac Koh (1005998) and Nicholas Teng (1003416)

## Folder Structure

In this project folder, you will find 2 folder, 6 Python Notebooks and 1 dataset csv.
- data_retrieval_and_cleaning folder :
    - This folder is used by us to get data from the various APIs. It contains our script to call data.gov.sg and NEA APIs and merging the individually gathered files into a merged dataset.
- final_models: 
    - This folder contains all the .pth that can be used to load our pre-trained models.
- 7 Python Notebooks:
    - The python notebooks that are numbered 1 to 6 all trains different models as the file name describes.
    - To retrain models, you can re-run the entire notebook except the cells that we have indicated that are for wandb hyperparameter tuning. These are the cells that have been commented out.
    - The final python notebook labeled 7 is for visualisation of all of our models. Just click "Run All"
- merged_file_with_mean.csv:
    - This is the dataset that we have curated and are using for all the model training.

## Installation

The project will require and has been tested on Python 3.12.4
Install the requirement.txt

```sh
pip install -r requirements.txt
```

You might need to still run the cells that contains !pip install just in case.

## Run and Test

As the project is entire on python notebooks, you can click Run All and the model should train and return you the results.

However, if you solely want to see the result, run the pytho notebook named: "7. visualisation_of_all_model_performance"

Clicking "Run All" here will load all of our models and return to you:
- MAE of that model
- Average Validation Loss
- A graph of a random batch that will allow you to see the predicted and target  

Thank you very much!