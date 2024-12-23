package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Response struct {
	Message string `json:"message"`
	Data    struct {
		Products []Product `json:"products"`
		Total    int       `json:"total"`
		Start    int       `json:"start"`
	} `json:"data"`
}

type Product struct {
	ProductID        string     `json:"product_id"`
	URL              string     `json:"url"`
	Language         string     `json:"language"`
	Title            string     `json:"title"`
	Type             string     `json:"type"`
	Description      string     `json:"description"`
	Categories       [][]string `json:"categories"`
	CoverImage       string     `json:"cover_image"`
	CustomAttributes struct {
		Publishers      []string `json:"publishers"`
		PublicationDate string   `json:"publication_date"`
	} `json:"custom_attributes"`
	Authors []string `json:"authors"`
}

const (
	pageSize      = 100
	pageMax       = 100
	maxConcurrent = 5 // Adjust as needed
)

func main() {
	baseURL := fmt.Sprintf("https://www.oreilly.com/search/api/search/?q=*&type=book&order_by=published_at&rows=%d&language=en&page=", pageSize)

	var allProducts []Product
	var wg sync.WaitGroup
	productsChan := make(chan []Product, maxConcurrent)

	// Fetch data concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		fetchProducts(baseURL, pageMax, &wg, productsChan)
		close(productsChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for products := range productsChan {
			allProducts = append(allProducts, products...)
		}
	}()

	wg.Wait()

	fileDate := time.Now().Format("2006-01-02")
	csvFilename := fmt.Sprintf("oreilly-book-list-%s.csv", fileDate)
	markdownFilename := fmt.Sprintf("oreilly-book-list-%s.md", fileDate)

	if err := writeCSV(csvFilename, allProducts); err != nil {
		log.Fatalf("Error writing CSV: %v", err)
	}

	if err := writeMarkdown(markdownFilename, allProducts); err != nil {
		log.Fatalf("Error writing Markdown: %v", err)
	}

	fmt.Println("Done.")
}

func fetchProducts(baseURL string, pageMax int, wg *sync.WaitGroup, productsChan chan<- []Product) {
	sem := make(chan struct{}, maxConcurrent) // Semaphore to limit concurrency

	for page := 0; page < pageMax; page++ {
		sem <- struct{}{} // Acquire a token
		wg.Add(1)

		go func(page int) {
			defer wg.Done()
			defer func() { <-sem }() // Release the token

			url := fmt.Sprintf("%s%d", baseURL, page)
			response, err := fetchData(url)
			if err != nil {
				log.Printf("Error fetching data from page %d: %v", page, err)
				return
			}

			log.Printf("page: %d, %s, %d", page, url, len(response.Data.Products))

			// Send the products to the channel
			productsChan <- response.Data.Products
		}(page)
	}
}

func fetchData(apiURL string) (Response, error) {
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return Response{}, err
	}

	req.Header.Add("referer", "https://www.oreilly.com/")
	req.Header.Add("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()

	var response Response
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return Response{}, err
	}

	return response, nil
}

func writeCSV(filename string, products []Product) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write CSV header
	header := []string{"Title", "Publication Date", "URL", "Type", "Language", "Categories", "Cover Image", "Publishers", "Authors"}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write product data to CSV
	for _, product := range products {
		categories := formatCategories(product.Categories)
		row := []string{
			product.Title,
			product.CustomAttributes.PublicationDate,
			product.URL,
			product.Type,
			product.Language,
			categories,
			product.CoverImage,
			fmt.Sprintf("%v", product.CustomAttributes.Publishers),
			fmt.Sprintf("%v", product.Authors),
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

// writeMarkdown writes product data to a Markdown file
func writeMarkdown(filename string, products []Product) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write Markdown header
	header := []string{"Title", "Publication Date", "Categories"}
	_, err = file.WriteString("| " + strings.Join(header, " | ") + " |\n")
	if err != nil {
		return err
	}

	// Write Markdown separator
	separator := make([]string, len(header))
	for i := range separator {
		separator[i] = "---"
	}
	_, err = file.WriteString("| " + strings.Join(separator, " | ") + " |\n")
	if err != nil {
		return err
	}

	// Write product data to Markdown
	for _, product := range products {
		categories := formatCategories(product.Categories)

		item := fmt.Sprintf("| [%s](%s) | %s | %s |\n", product.Title, product.URL, product.CustomAttributes.PublicationDate, categories)
		_, err := file.WriteString(item)
		if err != nil {
			return err
		}
	}

	return nil
}

func formatCategories(categories [][]string) string {
	var formatted string
	for _, category := range categories {
		if len(category) > 0 {
			formatted += fmt.Sprintf("%s > ", category[0])
		}
	}
	if len(formatted) > 0 {
		formatted = formatted[:len(formatted)-3] // Remove trailing " > "
	}
	return formatted
}
