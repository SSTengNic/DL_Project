# Taxi Availability Demand Forecasting

Built by Darren Chan (1006340), Isaac Koh (1005998) and Nicholas Teng (1003416)

## Folder Structure

In this project folder, you will find 2 folder, 6 Python Notebooks and 1 dataset csv.
- Final Report PDF :
    - This is our final report.
- data_retrieval_and_cleaning folder :
    - This folder is used by us to get data from the various APIs. It contains our script to call data.gov.sg and NEA APIs and merging the individually gathered files into a merged dataset.
- 7 Python Notebooks:
    - The python notebooks that are numbered 1 to 6 all trains different models as the file name describes.
    - To retrain models, you can re-run the entire notebook except the cells that we have indicated that are for wandb hyperparameter tuning. These are the cells that have been commented out.
    - The final python notebook labeled 7 is for visualisation of all of our models. To run this, you will need to download our pretrained models from the following Google Drive [link](https://drive.google.com/file/d/1lgrPe3VbCrH6XrMrt9VezLvgKJ3GP_sJ/view?usp=sharing), place the files inside the final_models folder and then click "Run All"
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

### For Training:
As the project is on python notebooks, you can click "Run All" on each individual python notebook and the model should train and return you the results.


### To see all the models performance:
However, if you solely want to see the result, run the python notebook named: "7. visualisation_of_all_model_performance"

For this you will need to download a zip file from the following Google Drive link that contains all our models:
https://drive.google.com/file/d/1lgrPe3VbCrH6XrMrt9VezLvgKJ3GP_sJ/view?usp=sharing

Once you download the zip file, unzip the contents and place them in the "final_models" folder.

Clicking "Run All" here will load all of our models and return to you:
- MAE of that model
- Average Validation Loss
- A graph of a random batch that will allow you to see the predicted and target  

Thank you very much!