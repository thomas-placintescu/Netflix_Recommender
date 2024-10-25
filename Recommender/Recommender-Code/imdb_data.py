from imdb import Cinemagoer
import pandas as pd
import pickle 
from concurrent.futures import ThreadPoolExecutor, as_completed

class MovieTitlesParser:
    """
    A class to parse the 'movie_titles.csv' file from the Netflix Prize dataset.

    Attributes:
        file_path (str): The path to the 'movie_titles.csv' file.
        data (list): A list to store parsed movie data.
        imdb_info (list); A list containing information about movies / tv shows from the imdb database
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
            
class IMDBDataCollector:
    """
    A class to collect and store IMDb data for a list of movies.

    Attributes
    ----------
    imdb_info : list
        A list to store collected IMDb data for each movie.
    ia : Cinemagoer
        An instance of the Cinemagoer class to interact with IMDb.

    Methods
    -------
    __init__():
        Initializes the IMDBDataCollector with an empty imdb_info list and a Cinemagoer instance.
    fetch_movie_data(movie_name, year):
        Fetches movie data from IMDb for a given movie name and release year.
    get_imdb(movies_df):
        Concurrently retrieves IMDb data for movies listed in a DataFrame.
    """

    def __init__(self):
        """
        Initializes IMDBDataCollector with an empty imdb_info list and a Cinemagoer instance.
        """

        self.imdb_info = [] # List to store IMDb data for each movie
        self.ia = Cinemagoer() # Initialize Cinemagoer instance once to reuse across methods

    def fetch_movie_data(self, movie_name: str, year: int):
        """
        Fetches movie data from IMDb for a given movie name and release year.

        Parameters
        ----------
        movie_name : str
            The title of the movie to search for.
        year : int
            The release year of the movie.

        Returns
        -------
        dict or None
            A dictionary containing movie details if found, otherwise None.

        Notes
        -----
        - Searches for the movie by title and filters results by release year.
        - Retrieves detailed information for the first matching movie.
        """
        try:
            # Search for movies matching the given title
            movies = self.ia.search_movie(movie_name)
            for movie in movies:
                # Check if the release year matches
                if movie['year'] == year and movie['title'] == movie_name:
                    movie_id = movie.movieID
                    movie_details = self.ia.get_movie(movie_id)
                    # Build a dictionary with relevant movie data
                    return {
                        'Title': movie_name,
                        'Release Year': year,
                        'Type': movie_details.get('kind'),
                        'Genres': movie_details.get('genres', []),
                        'Director': [director.personID for director in movie_details.get('directors', [])],
                        'IMDB Rating': movie_details.get('rating', []),
                        'Cast': [actor.personID for actor in movie_details.get('cast', [])]
                    }
        except Exception as e:
            # Handle exceptions (e.g., network errors, data parsing issues)
            print(f"Error fetching {movie_name}, ({year}): {e}")

        return None # Return None if movie not found or an error occurred
    
    def get_imdb(self, movies_df: pd.DataFrame, batch_size: int=30, max_num_batches: int = 1e7):
        """
        Concurrently retrieves IMDb data for movies listed in a DataFrame in batches.

        Parameters
        ----------
        movies_df : pandas.DataFrame
            DataFrame containing 'Title' and 'ReleaseYear' columns for movies to search on IMDb.
        batch_size : int
            Integer containing the size of batches for ThreadPoolExecuter to fetch data.
        max_num_batches: int
            maximum number of batches to search to run.

        Notes
        -----
        - Uses ThreadPoolExecutor to fetch data concurrently.
        - Updates the imdb_info list with data for each found movie.
        - Prints progress updates showing the number of movies found.
        """
        # Use ThreadPoolExecutor to fetch data concurrently - set max workers to 10 so that IMDb is not overloaded
        batch_start_index = 0
        batch_counter = 0

        while batch_counter < max_num_batches and batch_start_index < len(movies_df['Title']) - batch_size:
            with ThreadPoolExecutor(max_workers=10) as executor:
                # Map each future to its corresponding movie title and year
                batch_titles = movies_df['Title'].iloc[batch_start_index:batch_start_index + batch_size]
                batch_years = movies_df['ReleaseYear'].iloc[batch_start_index:batch_start_index + batch_size]

                futures = {executor.submit(self.fetch_movie_data, movie_name, year):
                           (movie_name, year) for movie_name, year in zip(
                                batch_titles,
                                batch_years)}
                # Process completed futures as they become available
                for future in as_completed(futures):
                    movie_name, year = futures[future]  # Retrieve the original movie name and year
                    try:
                        result = future.result()  # Get the result of the future
                        if result:
                            self.imdb_info.append(result)  # Append the movie data to imdb_info
                            print(f"Movies Found: {len(self.imdb_info)}/{len(movies_df['Title'])}")  # Progress update
                    except Exception as e:
                        # Handle exceptions that occurred during data fetching
                        print(f"Error processing {movie_name} ({year}): {e}")
            batch_start_index += batch_size
            batch_counter += 1
            if batch_counter > max_num_batches:
                print(f"Reached max num batches {max_num_batches}, so aborting")
                break


if __name__ == "__main__":

    file_path = "/Users/Thomas/Recommender_Netflix/Recommender/Data/movie_titles.csv"

    parser = MovieTitlesParser(file_path)
    parser.parse_file()
    movie_titles = parser.get_dataframe()

    #movie_titles.head()

    collector = IMDBDataCollector()
    collector.get_imdb(movie_titles, max_num_batches = 10)
    #print(collector.imdb_info)
    # Save results
    with open('imdb_info.pkl', 'wb') as f:
        pickle.dump(collector.imdb_info, f)





