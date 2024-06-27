#!/usr/bin/env python3


# Author: Raymond Malicdem
# Description: Cleaning up data using Pandas
#               to make it understandable and 
#               suitable for data analysis


import sys
import pandas as pd
from sqlalchemy import create_engine


def main():
    if len(sys.argv) < 3:
        print("Usage: python program.py <path_of_list_movie> <path_of_movies_db_to_create>")
        return

    # start of code that can be run in an Interactive
    # Python or Jupyter Notebook
    data = pd.read_csv(sys.argv[1])

    data = data.loc[:, "title":]
    data["otitle"] = data["title"]

    allgenres = data["genres"].str.split("|").explode().unique()

    data["year"] = data["title"].str.extract(r"([\(][0-9]+[\)])")
    data["year"] = data["year"].str.extract(r"([0-9]+)").fillna("1995")
    
    data["title"] = data["title"].str.replace(r"([\(][0-9]+[\)])", "", regex=True).str.rstrip()
    data["engtitle"] = data["title"].str.replace(r"([\(][\w\d\s,./!-:']+[\)])", "", regex=True)
    data["engtitle"] = data["engtitle"].str.strip()
    data["engtitlerearrange"] = data["engtitle"].str.split(",").str[::-1].apply(lambda x: [i.strip() for i in x]).str.join(" ")

    data["engtitleparenthesis"] = data["title"].str.extract(r"([\(][\w\s,./!-:']+[\)])").fillna("")
    repl = lambda m: m.group(1).replace(",", "")
    data["engtitleparenthesis"] = data["engtitleparenthesis"].str.replace(r"([0-9]+,)", repl, regex=True)
    data["engtitleparenthesisrearrange"] = data["engtitleparenthesis"].str.replace(r"[\(\)]", "", regex=True)
    data["engtitleparenthesisrearrange"] = data["engtitleparenthesisrearrange"].str.split(",").str[::-1]
    data["engtitleparenthesisrearrange"] = data["engtitleparenthesisrearrange"].apply(
                                                lambda x: [i.strip() for i in x]).str.join(" ")
    data["engtitleparenthesisrearrange"] = data["engtitleparenthesisrearrange"].apply(
                                                lambda x: (x if x == "" else f"({x})"))
    data["engtitlecat"] = data["engtitlerearrange"].str.cat(data["engtitleparenthesisrearrange"], sep=" ")
    data["title"] = data["engtitlecat"].str.strip()
    data["genre"] = data["genres"].str.split("|").str.get(0)

    data = data.drop(columns=
                     [
                         "otitle", 
                         "engtitle", 
                         "engtitlerearrange", 
                         "engtitleparenthesis",
                         "engtitleparenthesisrearrange",
                         "engtitlecat", 
                         "genres"
                    ]
                )

    data = data[["title", "genre", "year"]]

    engine = create_engine(f'sqlite:///{sys.argv[2]}',
                            echo=False)

    data.to_csv("movie_collection.csv", sep="|", columns=["title", "genre", "year"], 
                     index=False, header=True, mode="w")

    genres = pd.DataFrame({"genre": allgenres})
    genres.to_csv("movie_genres.csv", sep="|", columns=["genre"], index=False, header=True, mode="w")

    genres.to_sql('genres', con=engine)
    data[["title", "genre", "year"]].to_sql('movies', con=engine)


if __name__ == "__main__":
    main()
