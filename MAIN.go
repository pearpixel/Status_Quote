package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"encoding/json"
	"database/sql"
	"fmt"
	"strings"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type quote_t struct {
	ID string `json:"id"`
	Author string `json:"author"`
	Text string `json:"text"`
	CategoryName string `json:"category_name"`
	CategoryId int32 `json:"category_id"`
	ImageName string `json:"image"`
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

type reqtype_e int
const (
	GET_ALLQUOTES reqtype_e = iota
	GET_QUOTE
	GET_RANDQUOTE
	POST_QUOTE
	PUT_QUOTE
	DELETE_QUOTE

	GET_ALLCATS
	GET_CAT
	POST_CAT
	DELETE_CAT
)

func marshal_nullstring(ns *sql.NullString, dest *string) {	
	if ns.Valid {
		*dest = ns.String
	} else {
		*dest = ""
	}
}

func marshal_nullint32(ni *sql.NullInt32, dest *int32) {
	if ni.Valid {
		*dest = ni.Int32
	} else {
		*dest = 0
	}
}

func trim_string(s string) sql.NullString {
	var ns sql.NullString

	trimmed := strings.TrimSpace(s)

	if len(trimmed) == 0 {
		ns = sql.NullString{Valid: false}
	} else {
		ns = sql.NullString{Valid: true, String: trimmed}
	}

	return ns
}

func probe_fallback(nsCatName *sql.NullString, nsImageName *sql.NullString, quote *quote_t) {
	if !nsImageName.Valid {
		if !nsCatName.Valid {
			quote.ImageName = ""
			return
		}
		
		fbpath := get_fbpath(nsCatName.String)
		_, err := os.Stat(fbpath)

		if err == nil {
			quote.ImageName = fbpath
			return
		} 

		quote.ImageName = ""
		return
	}

	path := get_path(nsImageName.String)
	quote.ImageName = path
}

func read_quote_from_rows(rows pgx.Rows) (quote_t, error) {
	var qt quote_t
	var nsImagename sql.NullString
	var nsCategory sql.NullString
	var niCatId sql.NullInt32

	err := rows.Scan(&qt.ID, &qt.Author, &qt.Text, &nsImagename, &nsCategory, &niCatId)

	if err != nil {
		return qt, err
	}

	marshal_nullstring(&nsCategory, &qt.CategoryName)
	marshal_nullint32(&niCatId, &qt.CategoryId)

	probe_fallback(&nsCategory, &nsImagename, &qt)

	return qt, nil
}

func read_quote_from_row(row pgx.Row) (quote_t, error) {
	var qt quote_t
	var nsImagename sql.NullString
	var nsCategory sql.NullString
	var niCatId sql.NullInt32

	err := row.Scan(&qt.ID, &qt.Author, &qt.Text, &nsImagename, &nsCategory, &niCatId)

	if err != nil {
		return qt, err
	}

	marshal_nullstring(&nsCategory, &qt.CategoryName)
	marshal_nullint32(&niCatId, &qt.CategoryId)

	probe_fallback(&nsCategory, &nsImagename, &qt)

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

func acquire_dbconn(c *gin.Context, dbpool *pgxpool.Pool) (context.Context, context.CancelFunc, *pgxpool.Conn, error) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10 * time.Second)
	dbconn, err := dbpool.Acquire(ctx)
	return ctx, cancel, dbconn, err
}

func get_path(name string) string {
	return fmt.Sprintf("/pictures/%s.file", name)
}

func get_fbpath(name string) string {
	return fmt.Sprintf("pictures/fb_%s.file", name)
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

func timed_error(c *gin.Context) {
	c.JSON(http.StatusInternalServerError, gin.H{"error": "could not fulfill request at this time"})
}

func error_response(c *gin.Context, code int, s string) {
	c.JSON(code, gin.H{"error": s})
}

func picture_handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fsPath := filepath.Join("./pictures", r.URL.Path)

		info, err := os.Stat(fsPath)
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}

		if info.IsDir() {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	})
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
	config, err := pgxpool.ParseConfig(dburl)
	if err != nil {
		fatal_log("Database connection failed, [Error:] %v", err)
		return
	}
	config.MaxConns = 20
	config.MinConns = 2
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute

	dbpool, err := pgxpool.NewWithConfig(dbctx, config)
	if err != nil {
		fatal_log("Database connection failed, [Error:] %v", err)
		dbpool.Close()
		return
	}
	defer dbpool.Close()

	router := gin.Default();

	router.GET("/qt", func(c *gin.Context) {
		dispatch_request(c, dbpool, &quoteQueries, &catQueries, GET_ALLQUOTES)
	})

	router.GET("/qt/:id", func(c *gin.Context) {
		dispatch_request(c, dbpool, &quoteQueries, &catQueries, GET_QUOTE)
	})

	router.GET("/qt/rand", func(c *gin.Context) {
		dispatch_request(c, dbpool, &quoteQueries, &catQueries, GET_RANDQUOTE)
	})

	router.POST("/qt", func(c *gin.Context) {
		dispatch_request(c, dbpool, &quoteQueries, &catQueries, POST_QUOTE)
	})

	router.PUT("/qt/:id", func(c *gin.Context) {
		dispatch_request(c, dbpool, &quoteQueries, &catQueries, PUT_QUOTE)
	})

	router.DELETE("/qt/:id", func(c *gin.Context){ 
		dispatch_request(c, dbpool, &quoteQueries, &catQueries, DELETE_QUOTE)
	})

	router.GET("/cat", func(c *gin.Context) {
		dispatch_request(c, dbpool, &quoteQueries, &catQueries, GET_ALLCATS)
	})

	router.GET("/cat/:id", func(c *gin.Context){
		dispatch_request(c, dbpool, &quoteQueries, &catQueries, GET_CAT)
	})

	router.POST("/cat", func(c *gin.Context){
		dispatch_request(c, dbpool, &quoteQueries, &catQueries, POST_CAT)
	})

	router.DELETE("/cat/:id", func(c *gin.Context){
		dispatch_request(c, dbpool, &quoteQueries, &catQueries, DELETE_CAT)
	})

	fs := http.FileServer(http.Dir("./pictures"))
	phandler := picture_handler(fs)
	
	http.Handle("/pictures/", http.StripPrefix("/pictures/", /*http.FileServer(http.Dir("pictures"))*/ phandler))
	go func() { 
		http.ListenAndServe(":8081", nil)
	}()

	router.Run(":8080")
}

func dispatch_request(c *gin.Context, dbpool *pgxpool.Pool, quoteQueries *quote_queries_t, catQueries *cat_queries_t, requestType reqtype_e) {
	ctx, cancel, dbconn, err := acquire_dbconn(c, dbpool)
	defer cancel()
	if err != nil {
		dbconn.Release()
		print("[{%d} Error acquiring connection from pool] %v", requestType, err)
		timed_error(c)
		return
	}
	defer dbconn.Release()

	tx, err := dbconn.Begin(ctx)
	if err != nil {
		print("[{%d} Error creating tx from connection] %v", requestType, err)
		internal_error(c)
		return
	}
	defer tx.Rollback(ctx)

	select {
	case <- ctx.Done():
		print("[{%d} Selection] %v", requestType, err)
		internal_error(c)
		return
	default:
	}

	// 
	// 
	// 

	switch(requestType){
	case GET_ALLQUOTES:
		err = all_quotes(c, dbconn, ctx, quoteQueries)
		break

	case GET_QUOTE:
		idparam, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			error_response(c, http.StatusBadRequest, "invalid quote id")
			return
		}
		err = cherrypick_quote(c, dbconn, ctx, quoteQueries, idparam)
		break

	case GET_RANDQUOTE:
		err = random_quote(c, dbconn, ctx, quoteQueries)
		break

	case POST_QUOTE:
		err = post_quote_http(c, dbconn, ctx, quoteQueries)
		break

	case PUT_QUOTE:
		idparam, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			error_response(c, http.StatusBadRequest, "invalid quote id")
			return
		}
		err = put_quote_http(c, dbconn, ctx, quoteQueries, idparam)
		break

	case DELETE_QUOTE:
		idparam, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			error_response(c, http.StatusBadRequest, "invalid quote id")
			return
		}
		err = delete_quote(c, dbconn, ctx, quoteQueries, idparam)
		break

	case GET_ALLCATS:
		err = all_cats(c, dbconn, ctx, catQueries)
		break

	case GET_CAT:
		idparam, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			error_response(c, http.StatusBadRequest, "invalid quote id")
			return
		}
		err = cherrypick_cat(c, dbconn, ctx, catQueries, idparam)
		break

	case POST_CAT:
		err = post_cat_http(c, dbconn, ctx, catQueries)
		break

	case DELETE_CAT:
		idparam, err := strconv.Atoi(c.Param("id"))
		if err != nil {
			error_response(c, http.StatusBadRequest, "invalid quote id")
			return
		}
		err = delete_cat(c, dbconn, ctx, catQueries, idparam)
		break
	}

	if err != nil {
		return
	} 
	if err = tx.Commit(ctx); err != nil {
		print("[{%d} Commit Error] %v", requestType, err)
		internal_error(c)
	}
}

func all_quotes(ct *gin.Context, dbconn *pgxpool.Conn, dbctx context.Context, dbQueries *quote_queries_t) error {
	rows, err := dbconn.Query(dbctx, dbQueries.All)
	
	if err != nil {
		rows.Close()
		print("[all_quotes Error:] %v", err)
		internal_error(ct)
		return err
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		print("[all_quotes Error:] %v", err)
		internal_error(ct)
		return err
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
	return nil
}

func cherrypick_quote(ct *gin.Context, dbconn *pgxpool.Conn, dbctx context.Context, dbQueries *quote_queries_t, id int) error {
	row := dbconn.QueryRow(dbctx, dbQueries.Cherryp, id)

	qt, err := read_quote_from_row(row)

	if err != nil {
		errMsg := err.Error()

		if strings.Contains(errMsg, "no rows") {
			ct.Status(http.StatusNotFound)
			return err;
		}

		print("[cherrypick_quote Error:] %v", err)
		internal_error(ct)
		return err
	}

	ct.JSON(http.StatusOK, qt)
	return nil
}

func random_quote(ct *gin.Context, dbconn *pgxpool.Conn, dbctx context.Context, dbQueries *quote_queries_t) error {
	row := dbconn.QueryRow(dbctx, dbQueries.Rand)

	qt, err := read_quote_from_row(row)

	if err != nil {
		print("[random_quote Error:] %v", err)
		internal_error(ct)
		return err
	}

	ct.JSON(http.StatusOK, qt)
	return nil
}

func post_quote_http(ct *gin.Context, dbconn *pgxpool.Conn, dbctx context.Context, dbQueries *quote_queries_t) error {
	var qtParam quote_t

	if err := ct.BindJSON(&qtParam); err != nil {
		error_response(ct, http.StatusBadRequest, "data format error")
		return err
	}

	category := trim_string(qtParam.CategoryName)
	var catId sql.NullInt32

	if category.Valid {
		catIdc, err := strconv.Atoi(category.String)

		if err != nil {
			error_response(ct, http.StatusBadRequest, "invalid category id")
			return err
		}

		catId = sql.NullInt32{Valid: true, Int32: int32(catIdc)}
	}

	imageName := trim_string(qtParam.ImageName)

	err := dbconn.QueryRow(dbctx, dbQueries.New, qtParam.Author, qtParam.Text, catId, imageName).Scan(&qtParam.ID)

	if err != nil {
		errMsg := err.Error()

		if strings.Contains(errMsg, "SQLSTATE 23503") {
			error_response(ct, http.StatusBadRequest, "invalid category id")
			return err
		}

		print("[post_quote_http Error:] %v", err)
		internal_error(ct)
		return err
	}

	ct.JSON(http.StatusCreated, qtParam)
	return nil
}

func put_quote_http(ct *gin.Context, dbconn *pgxpool.Conn, dbctx context.Context, dbQueries *quote_queries_t, id int) error {
	var qtParam quote_t

	if err := ct.BindJSON(&qtParam); err != nil {
		error_response(ct, http.StatusBadRequest, "data fromat error")
		return err
	}

	category := trim_string(qtParam.CategoryName)
	var catId sql.NullInt32

	if category.Valid {
		catIdc, err := strconv.Atoi(category.String)

		if err != nil {
			error_response(ct, http.StatusBadRequest, "invalid category id")
			return err
		}

		catId = sql.NullInt32{Valid: true, Int32: int32(catIdc)}
	}

	imageName := trim_string(qtParam.ImageName)

	tag, err := dbconn.Exec(dbctx, dbQueries.Change, qtParam.Author, qtParam.Text, catId, imageName, id)

	if err != nil {
		errMsg := err.Error()

		if strings.Contains(errMsg, "SQLSTATE 23503") {
			error_response(ct, http.StatusBadRequest, "invalid category id")
			return err
		}

		print("[put_quote_http Error:] %v", err)
		internal_error(ct)
		return err
	}

	if tag.RowsAffected() > 0 {
		ct.Status(http.StatusOK)
	} else {
		ct.Status(http.StatusNotFound)
	}

	return nil
}

func delete_quote(ct *gin.Context, dbconn *pgxpool.Conn, dbctx context.Context, dbQueries *quote_queries_t, id int) error {
	tag, err := dbconn.Exec(dbctx, dbQueries.Delete, id)

	if err != nil {
		print("[delete_quote Error:] %v", err)
		internal_error(ct)
		return err
	}

	if tag.RowsAffected() > 0 {
		ct.Status(http.StatusNoContent)
	} else {
		ct.Status(http.StatusNotFound)
	}

	return nil
}

func all_cats(ct *gin.Context, dbconn *pgxpool.Conn, dbctx context.Context, dbQueries *cat_queries_t) error {
	rows, err := dbconn.Query(dbctx, dbQueries.All)

	if err != nil {
		rows.Close()
		print("[all_cats Error:] %v", err)
		internal_error(ct)
		return err
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		print("[all_cats Error:] %v", err)
		internal_error(ct)
		return err
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

	return nil
}

func cherrypick_cat(ct *gin.Context, dbconn *pgxpool.Conn, dbctx context.Context, dbQueries *cat_queries_t, id int) error {
	row := dbconn.QueryRow(dbctx, dbQueries.Cherryp, id)

	cat, err := read_cat_from_row(row)

	if err != nil {
		errMsg := err.Error()

		if strings.Contains(errMsg, "no rows") {
			ct.Status(http.StatusNotFound)
			return err
		}

		print("[cherrypick_cat Error:] %v", err)
		internal_error(ct)
		return err
	}

	ct.JSON(http.StatusOK, cat)

	return nil
}

func post_cat_http(ct *gin.Context, dbconn *pgxpool.Conn, dbctx context.Context, dbQueries *cat_queries_t) error {
	var catParam cat_t

	if err := ct.BindJSON(&catParam); err != nil {
		error_response(ct, http.StatusBadRequest, "data format error")
		return err
	}

	name := trim_string(catParam.Name)

	err := dbconn.QueryRow(dbctx, dbQueries.New, name).Scan(&catParam.ID)

	if err != nil {
		errMsg := err.Error()

		if strings.Contains(errMsg, "SQLSTATE 23502") {
			error_response(ct, http.StatusBadRequest, "invalid category id")
			return err
		}

		print("[post_cat_http Error:] %v", err)
		internal_error(ct)
		return err
	}

	ct.JSON(http.StatusCreated, catParam)

	return nil
}

func delete_cat(ct *gin.Context, dbconn *pgxpool.Conn, dbctx context.Context, dbQueries *cat_queries_t, id int) error {
	tag, err := dbconn.Exec(dbctx, dbQueries.Delete, id)

	if err != nil {
		print("[delete_cat Error:] %v", err)
		internal_error(ct)
		return err
	}

	if tag.RowsAffected() > 0 {
		ct.Status(http.StatusNoContent)
	} else {
		ct.Status(http.StatusNotFound)
	}

	return nil
}
