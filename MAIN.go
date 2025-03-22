package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"encoding/json"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

type quote struct {
	ID string `json:"id"`
	Author string `json:"author"`
	Text string `json:"text"`
}

type queries struct {
	Qall string `json:"qtall"`
	Qid string `json:"qt.id"`
	Qnew string `json:"qtnew"`
	Qchg string `json:"qtchange"`
	Qdel string `json:"qtdelete"`
}

func main() {
	// gin.SetMode(gin.ReleaseMode);

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Could not load .env file: %v", err)
		return
	}

	dburl := os.Getenv("DB_URL")
	if dburl == "" {
		log.Fatal(".env file missing DB_URL")
		return
	}

	file, err := os.ReadFile("dbqueries.json")
	if err != nil {
		log.Fatalf("Could not load dbqueries.json")
		return
	}

	var dbQueries queries 
	err = json.Unmarshal(file, &dbQueries)
	if err != nil {
		log.Fatalf("Could not parse dbqueries.json")
		return
	}

	dbctx := context.Background()
	dbconn, err := pgx.Connect(dbctx, dburl)
	if err != nil {
		log.Fatalf("database connection failed: %v", err)
		dbconn.Close(dbctx)
		return
	}
	defer dbconn.Close(dbctx)

	router := gin.Default();

	router.GET("/qtall", func(c *gin.Context) {
		// c.IndentedJSON(http.StatusOK, quotes)
		// c.JSON(http.StatusOK, quotes)
		qrows, err := dbconn.Query(dbctx, dbQueries.Qall)
		if err != nil {
			qrows.Close()
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if err = qrows.Err(); err != nil {
			qrows.Close()
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer qrows.Close()

		var qQuotes []quote

		for qrows.Next() {
			var q quote
			
			err = qrows.Scan(&q.ID, &q.Author, &q.Text)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			
			qQuotes = append(qQuotes, q)
		}

		c.JSON(http.StatusOK, qQuotes)
	})
	router.GET("/qt/:id", func(c *gin.Context) {
		idparam := c.Param("id");

		var q quote

		err = dbconn.QueryRow(dbctx, dbQueries.Qid, idparam).Scan(&q.ID, &q.Author, &q.Text)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, q)		
	})
	router.POST("/qtnew", func(c *gin.Context){
		var quoteparam quote

		if err := c.BindJSON(&quoteparam); err != nil {
			// received invalid json data
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		_, err := dbconn.Exec(dbctx, dbQueries.Qnew, quoteparam.Author, quoteparam.Text)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusCreated, quoteparam)
	})
	router.PUT("/qtchange", func(c *gin.Context){
		// this needs to check which feels are provided and only update them. BindJSON will leave non-provided empty

		var q quote
		if err := c.BindJSON(&q); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		id := c.Param("id")
		_, err := dbconn.Exec(dbctx, dbQueries.Qchg, q.Author, q.Text, q.ID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		q.ID = id

		c.JSON(http.StatusOK, q)
	})
	router.DELETE("/qtdelete/:id", func(c *gin.Context){
		id := c.Param("id")

		_, err := dbconn.Exec(dbctx, dbQueries.Qdel, id)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.Status(http.StatusNoContent)
	})

	router.Run("localhost:8080")
}