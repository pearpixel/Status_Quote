package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"encoding/json"
	"database/sql"
	"fmt"
	"strings"
	"strconv"
	//"bytes"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

type quote struct {
	ID string `json:"id"`
	Author string `json:"author"`
	Text string `json:"text"`
	Category string `json:"category"`
	Imagename string `json:"image"`
}

type queries struct {
	All string `json:"ALL"`
	Cherryp string `json:"CHERRYPICK"`
	Rand string `json:"RAND"`
	New string `json:"SUBMIT"`
	Change string `json:"CHANGE"`
	Delete string `json:"REMOVE"`
}

func marshal_nullstring(ns *sql.NullString, dest *string) {	
	if ns.Valid {
		*dest = ns.String
	} else {
		*dest = ""
	}
}

func read_quote_from_rows(rows pgx.Rows) (quote, error) {
	var qt quote
	var nsImagename sql.NullString
	var nsCategory sql.NullString

	err := rows.Scan(&qt.ID, &qt.Author, &qt.Text, &nsImagename, &nsCategory)

	if err != nil {
		return qt, err
	}

	marshal_nullstring(&nsCategory, &qt.Category)
	marshal_nullstring(&nsImagename, &qt.Imagename)

	if strings.TrimSpace(qt.Imagename) == "" { // if it doesn't have a file assigned, give it the category's fallback one IF that exists
		if strings.TrimSpace(qt.Category) != "" {
			qt.Imagename = fmt.Sprintf("fb_%s", &qt.Category)
		} else { 
			return qt, nil
		}
	}	

	pathString := get_path(qt.Imagename)
	qt.Imagename = pathString

	return qt, nil
}

func read_quote_from_row(row pgx.Row) (quote, error) {
	var qt quote
	var nsImagename sql.NullString
	var nsCategory sql.NullString

	err := row.Scan(&qt.ID, &qt.Author, &qt.Text, &nsImagename, &nsCategory)

	if err != nil {
		return qt, err
	}

	marshal_nullstring(&nsCategory, &qt.Category)
	marshal_nullstring(&nsImagename, &qt.Imagename)

	if strings.TrimSpace(qt.Imagename) == "" { // if it doesn't have a file assigned, give it the category's fallback one IF that exists
		if strings.TrimSpace(qt.Category) != "" {
			qt.Imagename = fmt.Sprintf("fb_%s", &qt.Category)
		} else { 
			return qt, nil
		}
	}	

	pathString := get_path(qt.Imagename)
	qt.Imagename = pathString

	return qt, nil
}

func get_path(name string) string {
	return fmt.Sprintf("/pictures/%s.file", name)
}

func print(format string, args ...interface{}) {
	log.Println(fmt.Sprintf(format, args...))
}

func fatal_log(format string, args ...interface{}) {
	log.Fatalf(fmt.Sprintf(format, args...))
}

func internal_error(c *gin.Context) {
	c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
}

func main() {
	gin.SetMode(gin.ReleaseMode);
	
	if err := godotenv.Load(); err != nil {
		fatal_log("Could not load .env file. [Error:] %v", err)
		return
	}

	dburl := os.Getenv("DB_URL")
	if dburl == "" {
		fatal_log(".env file missing DB_URL")
		return
	}

	file, err := os.ReadFile("dbqueries.json")
	if err != nil {
		fatal_log("Could not load dbqueries.json. [Error:] %v", err)
		return
	}

	var dbQueries queries 
	if err := json.Unmarshal(file, &dbQueries); err != nil {
		fatal_log("Could not parse dbqueries.json. [Error:] %v", err)
		return
	}

	dbctx := context.Background()
	dbconn, err := pgx.Connect(dbctx, dburl)
	if err != nil {
		fatal_log("Database connection failed, [Error:] %v", err)
		dbconn.Close(dbctx)
		return
	}
	defer dbconn.Close(dbctx)

	router := gin.Default();

	router.GET("/qt", func(c *gin.Context) {
		all_queries(c, dbconn, dbctx, &dbQueries)
	})

	router.GET("/qt/:id", func(c *gin.Context) {
		idparam, err := strconv.Atoi(c.Param("id"))

		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid quote id"})
			return
		}

		cherrypick_query(c, dbconn, dbctx, &dbQueries, idparam)
	})

	router.GET("/qt/rand", func(c *gin.Context) {
		random_query(c, dbconn, dbctx, &dbQueries)
	})

	router.POST("/qt", func(c *gin.Context) {
		post_query_http(c, dbconn, dbctx, &dbQueries)
	})

	router.PUT("/qt/:id", func(c *gin.Context) {
		put_query_http(c, dbconn, dbctx, &dbQueries)
	})

	router.DELETE("/qt/:id", func(c *gin.Context){ 
		idparam, err := strconv.Atoi(c.Param("id"))

		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid quote id"})
			return
		}

		delete_query(c, dbconn, dbctx, &dbQueries, idparam)
	})
	
	http.Handle("/pictures/", http.StripPrefix("/pictures/", http.FileServer(http.Dir("pictures"))))
	go func() { 
		http.ListenAndServe("4001", nil)
	}()

	router.Run(":4000")
}


func all_queries(ct *gin.Context, dbconn *pgx.Conn, dbctx context.Context, dbQueries *queries) {
	rows, err := dbconn.Query(dbctx, dbQueries.All)
	
	if err != nil {
		rows.Close()
		print("[all_queries Error:] %v", err)
		internal_error(ct)
		return
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		print("[all_queries Error:] %v", err)
		internal_error(ct)
		return
	}

	defer rows.Close()

	var quotes []quote
	
	for rows.Next() {
		qt, err := read_quote_from_rows(rows)

		if err != nil {
			print("[all_queries Error:] %v", err)
			continue
		}

		quotes = append(quotes, qt)
	}

	ct.JSON(http.StatusOK, quotes)
}

func cherrypick_query(ct *gin.Context, dbConn *pgx.Conn, dbctx context.Context, dbQueries *queries, id int) {
	row := dbConn.QueryRow(dbctx, dbQueries.Cherryp, id)

	qt, err := read_quote_from_row(row)

	if err != nil { //@todo return http.StatusNotFound when no quote with id
		print("[cherrypick_query Error:] %v", err)
		internal_error(ct)
		return
	}

	ct.JSON(http.StatusOK, qt)
}

func random_query(ct *gin.Context, dbConn *pgx.Conn, dbctx context.Context, dbQueries *queries) {
	row := dbConn.QueryRow(dbctx, dbQueries.Rand)

	qt, err := read_quote_from_row(row)

	if err != nil {
		print("[random_query Error:] %v", err)
		internal_error(ct)
		return
	}

	ct.JSON(http.StatusOK, qt)
}

func post_query_http(ct *gin.Context, dbConn *pgx.Conn, dbctx context.Context, dbQueries *queries) {
	var qtParam quote

	if err := ct.BindJSON(&qtParam); err != nil {
		ct.JSON(http.StatusBadRequest, gin.H{"error": "data format error"})
		return
	}

	var category int
	if len(qtParam.Category) == 0 {
		category = -1
	}

	if category != -1 {
		categoryN, err := strconv.Atoi(qtParam.Category)

		if err != nil {
			ct.JSON(http.StatusBadRequest, gin.H{"error": "data format error: invalid category id"})
			return
		}

		category = categoryN
	}

	var err error
	if category == -1 {
		err = dbConn.QueryRow(dbctx, dbQueries.New, qtParam.Author, qtParam.Text, nil, qtParam.Imagename).Scan(&qtParam.ID)
	} else { 
		err = dbConn.QueryRow(dbctx, dbQueries.New, qtParam.Author, qtParam.Text, category, qtParam.Imagename).Scan(&qtParam.ID)
	}

	if err != nil {
		print("[post_query_http Error:] %v", err)
		internal_error(ct)
		return
	}

	ct.JSON(http.StatusCreated, qtParam)
}

func put_query_http(ct *gin.Context, dbConn *pgx.Conn, dbctx context.Context, dbQueries *queries) {
	var qtParam quote

	if err := ct.BindJSON(&qtParam); err != nil {
		ct.JSON(http.StatusBadRequest, gin.H{"error": "data format error"})
		return
	}

	idParam, err := strconv.Atoi(ct.Param("id"))

	if err != nil {
		ct.JSON(http.StatusBadRequest, gin.H{"error": "invalid quote id"})
		return
	}

	var category int
	if len(qtParam.Category) == 0 {
		category = -1
	}

	if category != -1 {
		categoryN, err := strconv.Atoi(qtParam.Category)

		if err != nil {
			ct.JSON(http.StatusBadRequest, gin.H{"error": "data format error: invalid category id"})
			return
		}

		category = categoryN
	}

	if category == -1 {
		_, err = dbConn.Exec(dbctx, dbQueries.Change, qtParam.Author, qtParam.Text, nil, qtParam.Imagename, idParam)
	} else {
		_, err = dbConn.Exec(dbctx, dbQueries.Change, qtParam.Author, qtParam.Text, category, qtParam.Imagename, idParam)
	}

	if err != nil {
		print("[put_query_http Error:] %v", err)
		internal_error(ct)
		return
	}

	// qtParam.ID = idParam

	ct.JSON(http.StatusOK, qtParam)
}

func delete_query(ct *gin.Context, dbConn *pgx.Conn, dbctx context.Context, dbQueries *queries, id int) {
	_, err := dbConn.Exec(dbctx, dbQueries.Delete, id)

	if err != nil {
		print("[delete_query Error:] %v", err)
		internal_error(ct)
		return
	}

	ct.Status(http.StatusNoContent)
}
