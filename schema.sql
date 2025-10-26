CREATE DATABASE movies_db;
use movies_db;

-- MOVIES Table: 
CREATE TABLE movies (
    movieId INT PRIMARY KEY,
    title VARCHAR(512) NOT NULL, 
    genres VARCHAR(512), 
    director VARCHAR(255),
    plot TEXT,
    box_office BIGINT, 
    release_year SMALLINT,
    decade SMALLINT 
);


-- RATINGS Table: 

CREATE TABLE ratings (
		userId INT NOT NULL,
		movieId INT NOT NULL,
		rating DECIMAL(2, 1) NOT NULL, 
		unix_timestamp BIGINT, 
	    PRIMARY KEY (userId, movieId), 
        -- Foreign key to link ratings back to the movie.
        FOREIGN KEY (movieId) REFERENCES movies(movieId) 
		   ON DELETE CASCADE
	);