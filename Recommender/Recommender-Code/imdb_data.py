from imdb import Cinemagoer
import pandas as pd

class MovieTitlesParser:
    """
    A class to parse the 'movie_titles.csv' file from the Netflix Prize dataset.

    Attributes:
        file_path (str): The path to the 'movie_titles.csv' file.
        data (list): A list to store parsed movie data.
    """

    def __init__(self, file_path: str):
        """
        Initializes the MovieTitlesParser with the given file path.

        Args:
            file_path (str): The path to the 'movie_titles.csv' file.
        """
        self.file_path = file_path
        self.data = []
        
        
    def parse_line(self, line: str):
        """
        Parses a single line from the 'movie_titles.csv' file.

        Splits the line into MovieID, ReleaseYear, and Title.

        Args:
            line (str): A line from the 'movie_titles.csv' file.

        Returns:
            list: A list containing MovieID, ReleaseYear, and Title.
        """
        # Remove leading/trailing whitespace
        line = line.strip()
        #split the line into maximum 3 parts
        parts = line.split(',', 2) #only split first 2 commas
        #Ensure the list has exactly 3 elements
        if len(parts) < 3:
            parts.extend([''] * (3 - len(parts)))

        return parts

    def parse_file(self):
        """
        Parses the entire 'movie_titles.csv' file and stores the data in the 'data' attribute.
        """
        #open file and separate into relative sections
        with open(self.file_path, 'r', encoding = 'ISO-8859-1') as file:
            for line in file:
                MovieID, ReleaseYear, Title = self.parse_line(line)
                self.data.append({
                    'MovieID': int(MovieID),
                    'ReleaseYear': int(ReleaseYear) if ReleaseYear.isdigit() else 0,
                    'Title': Title
                })
    
    def get_dataframe(self):
        """
        Converts the parsed data into a pandas DataFrame.

        Returns:
            pandas.DataFrame: A DataFrame containing the movie data.
        """
        df = pd.DataFrame(self.data)

        return df
            
            


file_path = "/Users/Thomas/Recommender_Netflix/Recommender/Data/movie_titles.csv"

parser = MovieTitlesParser(file_path)
parser.parse_file()
movie_titles = parser.get_dataframe()

# Display the first few rows
movie_titles.info()

