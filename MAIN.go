package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"encoding/json"
	"database/sql"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)


type quote struct {
	ID string `json:"id"`
	Author string `json:"author"`
	Text string `json:"text"`
	Category string `json:"category"`
}

type queries struct {
	Qall string `json:"ALL"`
	Qcp string `json:"CHERRYPICK"`
	Qrd string `json:"RAND"`
	Qnew string `json:"SUBMIT"`
	Qchg string `json:"CHANGE"`
	Qdel string `json:"REMOVE"`
}

func marshal_null(ns *sql.NullString, q *quote) {
	if ns.Valid {
		q.Category = ns.String
	} else {
		q.Category = ""
	}
}


func main() {
	// gin.SetMode(gin.ReleaseMode);
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Could not load .env file: %v", err)
		return
	}

	dburl := os.Getenv("DB_URL")
	if dburl == "" {
		log.Fatalf(".env file missing DB_URL")
		return
	}

	file, err := os.ReadFile("dbqueries.json")
	if err != nil {
		log.Fatalf("Could not load dbqueries.json")
		return
	}

	var dbQueries queries 
	if err := json.Unmarshal(file, &dbQueries); err != nil {
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

	router.GET("/qt", func(c *gin.Context) {
		// c.IndentedJSON(http.StatusOK, quotes)
		// c.JSON(http.StatusOK, quotes)
		qrows, err := dbconn.Query(dbctx, dbQueries.Qall)
		if err != nil {
			qrows.Close()
			log.Println("[Error ALPHA] ", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
			return
		}
		if err := qrows.Err(); err != nil {
			qrows.Close()
			log.Println("[Error BETA] ", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
			return
		}
		defer qrows.Close()

		var qQuotes []quote

		for qrows.Next() {
			var q quote
			var ns sql.NullString
			
			err = qrows.Scan(&q.ID, &q.Author, &q.Text, &ns)
			if err != nil {
				log.Println("[Error GAMMA] ", err.Error())
				continue
			}
			
			marshal_null(&ns, &q)

			qQuotes = append(qQuotes, q)
		}

		// jsonData, err := json.MarshalIndent(qQuotes, "", "  ")

		c.JSON(http.StatusOK, qQuotes)
	})
	router.GET("/qt/:id", func(c *gin.Context) {
		idparam := c.Param("id");

		var q quote
		var ns sql.NullString

		err = dbconn.QueryRow(dbctx, dbQueries.Qcp, idparam).Scan(&q.ID, &q.Author, &q.Text, &ns)
		if err != nil {
			log.Println("[Error DELTA] ", err.Error())
			c.JSON(http.StatusNotFound, gin.H{"error": "could not find item specified"})
			return
		}

		marshal_null(&ns, &q)

		c.JSON(http.StatusOK, q)		
	})
	router.GET("/qt/rand", func(c *gin.Context){
		var q quote
		var ns sql.NullString

		err = dbconn.QueryRow(dbctx, dbQueries.Qrd).Scan(&q.ID, &q.Author, &q.Text, &ns)
		if err != nil {
			log.Println("[Error KAPPA] ", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
			return
		}

		marshal_null(&ns, &q)

		c.JSON(http.StatusOK, q)
	})
	router.POST("/qt", func(c *gin.Context){
		var quoteparam quote

		if err := c.BindJSON(&quoteparam); err != nil {
			// received invalid json data
			log.Println("[Error EPSILON] ", err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": "data format error"})
			return
		}
		
		err := dbconn.QueryRow(dbctx, dbQueries.Qnew, quoteparam.Author, quoteparam.Text, quoteparam.Category).Scan(&quoteparam.ID)
		if err != nil {
			log.Println("[Error ZETA] ", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
			return
		}

		c.JSON(http.StatusCreated, quoteparam)
	})
	router.PUT("/qt/:id", func(c *gin.Context){
		// this needs to check which feels are provided and only update them. BindJSON will leave non-provided empty

		var q quote
		if err := c.BindJSON(&q); err != nil {
			log.Println("[Error ETA] ", err.Error())
			c.JSON(http.StatusBadRequest, gin.H{"error": "data format error"})
			return
		}

		idparam := c.Param("id")
		_, err := dbconn.Exec(dbctx, dbQueries.Qchg, q.Author, q.Text, q.Category, idparam)
		if err != nil {
			log.Println("[Error THETA] ", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
			return
		}

		q.ID = idparam

		c.JSON(http.StatusOK, q)
	})
	router.DELETE("/qt/:id", func(c *gin.Context){
		id := c.Param("id")

		_, err := dbconn.Exec(dbctx, dbQueries.Qdel, id)
		if err != nil {
			log.Println("[Error IOTA] ", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal sever error"})
			return
		}

		c.Status(http.StatusNoContent)
	})

	router.Run("localhost:8080")
}
