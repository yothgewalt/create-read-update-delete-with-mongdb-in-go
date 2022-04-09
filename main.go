package main

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"net/http"
	"time"
)

var client *mongo.Client

func main() {
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err = mongo.Connect(ctx, options.Client().ApplyURI("<uri>"))
	if err != nil {
		log.Fatalf("\nFailed to connect the database cause %v\n", err)
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatalln(err)
	}

	r := gin.Default()

	api := r.Group("/api", func(c *gin.Context) { c.Next() })
	api.GET("/v1/collections", readCollections())
	api.GET("/v1/collections/:name", readCollectionByName())
	api.POST("/v1/create/collection", writeCollection())
	api.PUT("/v1/update/collection/:id", updateCollectionById())
	api.DELETE("/v1/delete/collection/:name", deleteCollectionByName())

	r.Run(":3000")
}

func getCollection(client *mongo.Client, collectionName string) *mongo.Collection {
	collection := client.Database("sample").Collection(collectionName)
	return collection
}

func readCollections() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		collection := getCollection(client, "dataset")
		cursor, err := collection.Find(ctx, bson.D{})
		if err != nil {
			c.JSON(http.StatusInternalServerError, err)
			return
		}
		defer func(cursor *mongo.Cursor, ctx context.Context) {
			err := cursor.Close(ctx)
			if err != nil {
				c.JSON(http.StatusInternalServerError, err)
				return
			}
		}(cursor, ctx)

		var result []Fields
		if err = cursor.All(ctx, &result); err != nil {
			c.JSON(http.StatusInternalServerError, err)
			return
		}

		var results []Response
		for _, field := range result {
			r := Response{
				Username: field.Username,
				Password: field.Password,
			}

			results = append(results, r)
		}

		c.JSON(http.StatusFound, results)
	}
}

func readCollectionByName() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		name := c.Param("name")
		collection := getCollection(client, "dataset")

		var result Fields
		filter := bson.D{{"username", name}}
		err := collection.FindOne(ctx, filter).Decode(&result)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				c.JSON(http.StatusNotFound, err)
				return
			} else {
				c.JSON(http.StatusInternalServerError, err)
				return
			}
		}

		c.JSON(http.StatusFound, Response{
			Username: result.Username,
			Password: result.Password,
		})
	}
}

func writeCollection() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var collection Binding
		if err := c.ShouldBindJSON(&collection); err != nil {
			c.JSON(http.StatusBadRequest, err)
			return
		}

		cursor := getCollection(client, "dataset")
		document := bson.D{{"username", collection.Username}, {"password", collection.Password}}

		result, err := cursor.InsertOne(ctx, document)
		if err != nil {
			c.JSON(http.StatusInternalServerError, err)
			return
		}

		inserted := fmt.Sprintf("Inserted document with _id: %v", result.InsertedID)

		c.JSON(http.StatusCreated, gin.H{
			"result": inserted,
		})
	}
}

func updateCollectionById() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		id := c.Param("id")

		var update ChangeUsername
		if err := c.ShouldBindJSON(&update); err != nil {
			c.JSON(http.StatusBadRequest, err)
			return
		}

		objId, _ := primitive.ObjectIDFromHex(id)

		collection := getCollection(client, "dataset")

		updateWithStruct := bson.M{"username": update.Username}

		result, err := collection.UpdateOne(ctx, bson.M{"_id": objId}, bson.M{"$set": updateWithStruct})
		if err != nil {
			c.JSON(http.StatusInternalServerError, err)
			return
		}

		updated := fmt.Sprintf("count of modified document: %v", result.ModifiedCount)

		c.JSON(http.StatusOK, gin.H{
			"result": updated,
		})
	}
}

func deleteCollectionByName() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		name := c.Param("name")
		collection := getCollection(client, "dataset")

		filter := bson.D{{"username", name}}
		result, err := collection.DeleteOne(ctx, filter)
		if err != nil {
			c.JSON(http.StatusInternalServerError, err)
			return
		}

		deleted := fmt.Sprintf("count of deleted document: %v", result.DeletedCount)

		c.JSON(http.StatusOK, gin.H{
			"result": deleted,
		})
	}
}

type Fields struct {
	Username string
	Password string
}

type ChangeUsername struct {
	Username string `json:"username"`
}

type Binding struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type Response struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
