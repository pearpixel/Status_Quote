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

type quote_t struct {
	ID string `json:"id"`
	Author string `json:"author"`
	Text string `json:"text"`
	Category string `json:"category"`
	Imagename string `json:"image"`
}

type cat_t struct {
	ID string `json:"id"`
	Name string `json:"name"`
}

type quote_queries_t struct {
	All string `json:"ALL"`
	Cherryp string `json:"CHERRYPICK"`
	Rand string `json:"RAND"`
	New string `json:"SUBMIT"`
	Change string `json:"CHANGE"`
	Delete string `json:"REMOVE"`
}

type cat_queries_t struct {
	All string `json:"ALL"`
	Cherryp string `json:"CHERRYPICK"`
	New string `json:"SUBMIT"`
	Delete string `json:"REMOVE"`
}

func marshal_nullstring(ns *sql.NullString, dest *string) {	
	if ns.Valid {
		*dest = ns.String
	} else {
		*dest = ""
	}
}

func read_quote_from_rows(rows pgx.Rows) (quote_t, error) {
	var qt quote_t
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

func read_quote_from_row(row pgx.Row) (quote_t, error) {
	var qt quote_t
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

func read_cat_from_rows(rows pgx.Rows) (cat_t, error){
	var cat cat_t

	err := rows.Scan(&cat.ID, &cat.Name)

	return cat, err
}

func read_cat_from_row(row pgx.Row) (cat_t, error){
	var cat cat_t

	err := row.Scan(&cat.ID, &cat.Name)

	return cat, err
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
	// gin.SetMode(gin.ReleaseMode);
	
	if err := godotenv.Load(); err != nil {
		fatal_log("Could not load .env file. [Error:] %v", err)
		return
	}

	dburl := os.Getenv("DB_URL")
	if dburl == "" {
		fatal_log(".env file missing DB_URL")
		return
	}

	quotefile, err := os.ReadFile("dbqueries.json")
	if err != nil {
		fatal_log("Could not load dbqueries.json. [Error:] %v", err)
		return
	}

	var quoteQueries quote_queries_t 
	if err := json.Unmarshal(quotefile, &quoteQueries); err != nil {
		fatal_log("Could not parse dbqueries.json. [Error:] %v", err)
		return
	}

	catfile, err := os.ReadFile("catqueries.json")
	if err != nil {
		fatal_log("Could not load catqueries.json. [Error:] %v", err)
		return
	}

	var catQueries cat_queries_t
	if err := json.Unmarshal(catfile, &catQueries); err != nil {
		fatal_log("Could not parse catqueries.json. [Error:] %v", err)
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
		all_quotes(c, dbconn, dbctx, &quoteQueries)
	})

	router.GET("/qt/:id", func(c *gin.Context) {
		idparam, err := strconv.Atoi(c.Param("id"))

		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid quote id"})
			return
		}

		cherrypick_quote(c, dbconn, dbctx, &quoteQueries, idparam)
	})

	router.GET("/qt/rand", func(c *gin.Context) {
		random_quote(c, dbconn, dbctx, &quoteQueries)
	})

	router.POST("/qt", func(c *gin.Context) {
		post_quote_http(c, dbconn, dbctx, &quoteQueries)
	})

	router.PUT("/qt/:id", func(c *gin.Context) {
		put_quote_http(c, dbconn, dbctx, &quoteQueries)
	})

	router.DELETE("/qt/:id", func(c *gin.Context){ 
		idparam, err := strconv.Atoi(c.Param("id"))

		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid quote id"})
			return
		}

		delete_quote(c, dbconn, dbctx, &quoteQueries, idparam)
	})

	router.GET("/cat", func(c *gin.Context) {
		all_cats(c, dbconn, dbctx, &catQueries)
	})

	router.GET("/cat/:id", func(c *gin.Context){
		idparam, err := strconv.Atoi(c.Param("id"))

		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid category id"})
			return
		}

		cherrypick_cat(c, dbconn, dbctx, &catQueries, idparam)
	})

	router.POST("/cat", func(c *gin.Context){
		post_cat_http(c, dbconn, dbctx, &catQueries)
	})

	router.DELETE("/cat/:id", func(c *gin.Context){
		idparam, err := strconv.Atoi(c.Param("id"))

		
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid category id"})
			return
		}

		delete_cat(c, dbconn, dbctx, &catQueries, idparam)
	})
	
	http.Handle("/pictures/", http.StripPrefix("/pictures/", http.FileServer(http.Dir("pictures"))))
	go func() { 
		http.ListenAndServe("localhost:8081", nil)
	}()

	router.Run("localhost:8080")
}

func all_quotes(ct *gin.Context, dbconn *pgx.Conn, dbctx context.Context, dbQueries *quote_queries_t) {
	rows, err := dbconn.Query(dbctx, dbQueries.All)
	
	if err != nil {
		rows.Close()
		print("[all_quotes Error:] %v", err)
		internal_error(ct)
		return
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		print("[all_quotes Error:] %v", err)
		internal_error(ct)
		return
	}

	defer rows.Close()

	var quotes []quote_t
	
	for rows.Next() {
		qt, err := read_quote_from_rows(rows)

		if err != nil {
			print("[all_quotes Error:] %v", err)
			continue
		}

		quotes = append(quotes, qt)
	}

	ct.JSON(http.StatusOK, quotes)
}

func cherrypick_quote(ct *gin.Context, dbConn *pgx.Conn, dbctx context.Context, dbQueries *quote_queries_t, id int) {
	row := dbConn.QueryRow(dbctx, dbQueries.Cherryp, id)

	qt, err := read_quote_from_row(row)

	if err != nil { //@todo return http.StatusNotFound when no quote with id
		print("[cherrypick_quote Error:] %v", err)
		internal_error(ct)
		return
	}

	ct.JSON(http.StatusOK, qt)
}

func random_quote(ct *gin.Context, dbConn *pgx.Conn, dbctx context.Context, dbQueries *quote_queries_t) {
	row := dbConn.QueryRow(dbctx, dbQueries.Rand)

	qt, err := read_quote_from_row(row)

	if err != nil {
		print("[random_quote Error:] %v", err)
		internal_error(ct)
		return
	}

	ct.JSON(http.StatusOK, qt)
}

func post_quote_http(ct *gin.Context, dbConn *pgx.Conn, dbctx context.Context, dbQueries *quote_queries_t) {
	var qtParam quote_t

	if err := ct.BindJSON(&qtParam); err != nil {
		ct.JSON(http.StatusBadRequest, gin.H{"error": "data format error"})
		return
	}

	var category sql.NullInt32

	if len(qtParam.Category) == 0 {
		category = sql.NullInt32{Valid: false}
	} else {
		catId, err := strconv.Atoi(qtParam.Category)
		
		if err != nil {
			ct.JSON(http.StatusBadRequest, gin.H{"error": "invalid category id"})
			return
		}

		category = sql.NullInt32{Int32: int32(catId), Valid: true}
	}

	err := dbConn.QueryRow(dbctx, dbQueries.New, qtParam.Author, qtParam.Text, category, qtParam.Imagename).Scan(&qtParam.ID)

	if err != nil {
		errMsg := err.Error()

		if strings.Contains(errMsg, "SQLSTATE 23503") {
			ct.JSON(http.StatusBadRequest, gin.H{"error": "invalid category id"})
			return
		}

		print("[post_quote_http Error:] %v", err)
		internal_error(ct)
		return
	}

	ct.JSON(http.StatusCreated, qtParam)
}

func put_quote_http(ct *gin.Context, dbConn *pgx.Conn, dbctx context.Context, dbQueries *quote_queries_t) {
	var qtParam quote_t

	if err := ct.BindJSON(&qtParam); err != nil {
		ct.JSON(http.StatusBadRequest, gin.H{"error": "data format error"})
		return
	}

	idParam, err := strconv.Atoi(ct.Param("id"))

	if err != nil {
		ct.JSON(http.StatusBadRequest, gin.H{"error": "invalid quote id"})
		return
	}

	var category sql.NullInt32

	if len(qtParam.Category) == 0 {
		category = sql.NullInt32{Valid: false}
	} else {
		catId, err := strconv.Atoi(qtParam.Category)
		
		if err != nil {
			ct.JSON(http.StatusBadRequest, gin.H{"error": "invalid category id"})
			return
		}

		category = sql.NullInt32{Int32: int32(catId), Valid: true}
	}

	tag, err := dbConn.Exec(dbctx, dbQueries.Change, qtParam.Author, qtParam.Text, category, qtParam.Imagename, idParam)

	if err != nil {
		errMsg := err.Error()

		if strings.Contains(errMsg, "SQLSTATE 23503") {
			ct.JSON(http.StatusBadRequest, gin.H{"error": "invalid category id"})
			return
		}

		print("[put_quote_http Error:] %v", err)
		internal_error(ct)
		return
	}

	if tag.RowsAffected() > 0 {
		ct.Status(http.StatusOK)
	} else {
		ct.Status(http.StatusNotFound)
	}
}

func delete_quote(ct *gin.Context, dbConn *pgx.Conn, dbctx context.Context, dbQueries *quote_queries_t, id int) {
	tag, err := dbConn.Exec(dbctx, dbQueries.Delete, id)

	if err != nil {
		print("[delete_quote Error:] %v", err)
		internal_error(ct)
		return
	}

	if tag.RowsAffected() > 0 {
		ct.Status(http.StatusNoContent)
	} else {
		ct.Status(http.StatusNotFound)
	}
}

func all_cats(ct *gin.Context, dbConn *pgx.Conn, dbctx context.Context, dbQueries *cat_queries_t) {
	rows, err := dbConn.Query(dbctx, dbQueries.All)

	if err != nil {
		rows.Close()
		print("[all_cats Error:] %v", err)
		internal_error(ct)
		return
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		print("[all_cats Error:] %v", err)
		internal_error(ct)
		return
	}

	defer rows.Close()

	var cats []cat_t

	for rows.Next() {
		cat, err := read_cat_from_rows(rows)

		if err != nil {
			print("[all_cats Error:] %v", err)
			continue
		}

		cats = append(cats, cat)
	}

	ct.JSON(http.StatusOK, cats)
}

func cherrypick_cat(ct *gin.Context, dbConn *pgx.Conn, dbctx context.Context, dbQueries *cat_queries_t, id int) {
	row := dbConn.QueryRow(dbctx, dbQueries.Cherryp, id)

	cat, err := read_cat_from_row(row)

	if err != nil {
		errMsg := err.Error()

		if strings.Contains(errMsg, "no rows") {
			ct.JSON(http.StatusNotFound, gin.H{"error": "invalid category id"})
			return
		}

		print("[cherrypick_cat Error:] %v", err)
		internal_error(ct)
		return
	}

	ct.JSON(http.StatusOK, cat)
}

func post_cat_http(ct *gin.Context, dbConn *pgx.Conn, dbctx context.Context, dbQueries *cat_queries_t) {
	var catParam cat_t

	if err := ct.BindJSON(&catParam); err != nil {
		ct.JSON(http.StatusBadRequest, gin.H{"error": "data format error"})
		return
	}

	var name sql.NullString

	if strings.TrimSpace(catParam.Name) == "" {
		name = sql.NullString{Valid: false}
	} else {
		name = sql.NullString{Valid: true, String: catParam.Name}
	}

	err := dbConn.QueryRow(dbctx, dbQueries.New, name).Scan(&catParam.ID)

	if err != nil {
		errMsg := err.Error()

		if strings.Contains(errMsg, "SQLSTATE 23502") {
			ct.JSON(http.StatusBadRequest, gin.H{"error": "invalid category name"})
			return
		}

		print("[post_cat_http Error:] %v", err)
		internal_error(ct)
		return
	}

	ct.JSON(http.StatusCreated, catParam)
}

func delete_cat(ct *gin.Context, dbConn *pgx.Conn, dbctx context.Context, dbQueries *cat_queries_t, id int) {
	tag, err := dbConn.Exec(dbctx, dbQueries.Delete, id)

	if err != nil {
		print("[delete_cat Error:] %v", err)
		internal_error(ct)
		return
	}

	if tag.RowsAffected() > 0 {
		ct.Status(http.StatusNoContent)
	} else {
		ct.Status(http.StatusNotFound)
	}
}