import scrapy

class BooksSpider(scrapy.Spider):
    name = "books"
    start_urls = ["http://books.toscrape.com/"]

    def parse(self, response):
        for book in response.css("article.product_pod"):
            yield {
                "titre": book.css("h3 a::attr(title)").get(),
                "prix": book.css("p.price_color::text").get(),
                "disponible": book.css("p.instock.availability::text").getall()[-1].strip(),
                "image": response.urljoin(book.css("div.image_container img::attr(src)").get())
            }

        # Pagination
        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, self.parse)
