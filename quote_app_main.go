package main

import (
	"net/http"
	"github.com/gin-gonic/gin"
)

type quote struct {
	ID string `json:"id"`
	Author string `json:"author"`
	Text string `json:"text"`
}

var quotes = []quote{
	{ID: "0", Author: "der maxxe", Text: "JA DOCH"},
	{ID: "1", Author: "nikkis papa", Text: "Sicher ist sicher"},
	{ID: "2", Author: "maxxe nochma", Text: "Musst rauchen fuer ne schoene stimme"},
}

func main() {
	router := gin.Default();

	router.GET("/qtall", func(c *gin.Context) {
		// c.IndentedJSON(http.StatusOK, quotes)
		c.JSON(http.StatusOK, quotes)
	})
	router.GET("/qt/:id", func(c *gin.Context) {
		idparam := c.Param("id");

		for _, quote := range quotes {
			if quote.ID == idparam {
				c.JSON(http.StatusOK, quote)
				return;
			}
		}

		c.JSON(http.StatusNotFound, gin.H{"message": "quote not found"})
	})
	router.POST("/qt", func(c *gin.Context){
		var quoteparam quote
		if err := c.BindJSON(&quoteparam); err != nil {
			// received invalid json data
			c.JSON(http.StatusBadRequest, gin.H{"message": "bad data"})
		}

		quotes = append(quotes, quoteparam)
		c.JSON(http.StatusCreated, quoteparam)
	})

	router.Run("localhost:8080")
}