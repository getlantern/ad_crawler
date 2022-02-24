package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"gopkg.in/yaml.v2"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-shiori/go-readability"
)

type Article struct {
	ID    int64  `yaml:"id"`
	URL   string `yaml:"url"`
	Title string `yaml:"title"`
}

func (a Article) Filename() string {
	return fmt.Sprintf("%d.html", a.ID)
}

type FeedItem struct {
	URL  string
	Name string
}

const LanternAdsBucket = "lantern-ads"
const LanternAdsIndex = "index.yaml"

var PartnerFeeds = []string{
	"https://www.persagg.com/zh.yaml",
	//"https://www.persagg.com/zh-week.yaml",
	//"https://www.persagg.com/fa.yaml",
	//"https://www.persagg.com/fa-week.yaml",
}

func loadFeedItems() []FeedItem {
	var items []FeedItem
	for _, url := range PartnerFeeds {
		resp, err := http.Get(url)
		if err != nil {
			fmt.Println(err)
			continue
		}

		b, err := ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			fmt.Println(err)
			continue
		}
		var feed []FeedItem
		err = yaml.Unmarshal(b, &feed)
		if err != nil {
			fmt.Println(err)
			continue
		}
		items = append(items, feed...)
	}
	return items
}

func getCurrentIndex(client *s3.Client) (nextID int64, currentArticles []Article) {
	indexData, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(LanternAdsBucket),
		Key:    aws.String(LanternAdsIndex),
	})

	if err != nil {
		var responseError *awshttp.ResponseError
		if !errors.As(err, &responseError) || responseError.ResponseError.HTTPStatusCode() != http.StatusNotFound {
			panic(fmt.Errorf("problem accessing s3 bucket. check if env variables for AWS are set: %v", err))
		}
		// all good, no index yet
	} else {
		err := yaml.NewDecoder(indexData.Body).Decode(&currentArticles)
		defer func() { _ = indexData.Body.Close() }()
		if err != nil {
			panic(err)
		}
	}
	nextID = 1
	if len(currentArticles) > 0 {
		nextID = currentArticles[len(currentArticles)-1].ID + 1
	}
	return
}

func diffArticles(currentArticles []Article, feedItems []FeedItem, nextID int64) (articlesToDownload []Article, newIndex []Article) {
	wantUrl := func(url string) bool {
		for _, item := range feedItems {
			if item.URL == url {
				return true
			}
		}
		return false
	}
	newUrl := func(url string) bool {
		for _, item := range currentArticles {
			if item.URL == url {
				return false
			}
		}
		return true
	}
	for _, a := range currentArticles {
		// if current article url is still in the feed, keep it
		if wantUrl(a.URL) {
			newIndex = append(newIndex, a)
		}
	}
	for _, item := range feedItems {
		// if the feed item url is not in the current articles, queue it up for download
		if newUrl(item.URL) {
			a := Article{
				ID:    nextID,
				URL:   item.URL,
				Title: item.Name,
			}
			articlesToDownload = append(articlesToDownload, a)
			newIndex = append(newIndex, a)
			nextID++
		}
	}
	return
}

func updateIndex(index []Article, client *s3.Client) {
	sort.SliceStable(index, func(i, j int) bool {
		return index[i].ID < index[j].ID
	})
	data, _ := yaml.Marshal(index)

	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(LanternAdsBucket),
		Key:    aws.String(LanternAdsIndex),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		panic(err)
	}
}

func deleteArticles(newIndex []Article, client *s3.Client) int {
	resp, err := client.ListObjects(context.TODO(), &s3.ListObjectsInput{
		Bucket: aws.String(LanternAdsBucket),
	})

	if err != nil {
		fmt.Printf("Cannot list s3 bucket: %v", err)
		return 0
	}

	wantedArticle := func(fn string) bool {
		for _, item := range newIndex {
			if item.Filename() == fn {
				return true
			}
		}
		return false
	}

	deleteInput := &s3.DeleteObjectsInput{
		Bucket: aws.String(LanternAdsBucket),
		Delete: &types.Delete{
			Objects: nil,
			Quiet:   true,
		},
	}

	for _, item := range resp.Contents {
		if *item.Key != "index.yaml" && !wantedArticle(*item.Key) {
			deleteInput.Delete.Objects = append(deleteInput.Delete.Objects, types.ObjectIdentifier{
				Key: item.Key,
			})
		}
	}

	res, err := client.DeleteObjects(context.TODO(), deleteInput)

	if err != nil {
		fmt.Printf("Cannot delete old articles: %v", err)
	}
	return len(res.Deleted)
}

func downloadArticles(articlesToDownload []Article, newIndex []Article, client *s3.Client) uint64 {
	wg := sync.WaitGroup{}
	downloaded := uint64(0)
	var badIDs sync.Map

	for _, article := range articlesToDownload {
		wg.Add(1)
		go func(article Article) {
			defer wg.Done()
			a, err := readability.FromURL(article.URL, 30*time.Second)
			if err != nil {
				fmt.Printf("Unable to fetch origin article at %v: %v", article.URL, err)
				badIDs.Store(article.ID, true)
				return
			}
			_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: aws.String(LanternAdsBucket),
				Key:    aws.String(article.Filename()),
				Body:   bytes.NewReader([]byte(a.TextContent)),
			})
			if err != nil {
				fmt.Printf("Unable to upload article %v: %v", article.ID, err)
				badIDs.Store(article.ID, true)
				return
			}
			atomic.AddUint64(&downloaded, 1)
		}(article)
	}
	wg.Wait()
	// clean up the index from articles that failed to download
	i := 0 // output index
	for _, x := range newIndex {
		if _, ok := badIDs.Load(x.ID); ok {
			continue
		}
		// copy and increment index
		newIndex[i] = x
		i++
	}
	return downloaded
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}
	client := s3.NewFromConfig(cfg)

	items := loadFeedItems()
	nextID, currentArticles := getCurrentIndex(client)
	articlesToDownload, newIndex := diffArticles(currentArticles, items, nextID)
	deleted := deleteArticles(newIndex, client)
	downloaded := downloadArticles(articlesToDownload, newIndex, client)
	if deleted > 0 || downloaded > 0 {
		updateIndex(newIndex, client)
	}

}
