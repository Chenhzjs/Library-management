package entities;

import java.util.Comparator;
import java.util.Objects;
import java.util.Random;

public final class Book {
    private int bookId;
    private String category;
    private String title;
    private String press;
    private int publishYear;
    private String author;
    private double price;
    private int stock;

    public enum SortColumn {
        BOOK_ID("book_id", Comparator.comparingInt(Book::getBookId)),
        CATEGORY("category", Comparator.comparing(Book::getCategory)),
        TITLE("title", Comparator.comparing(Book::getTitle)),
        PRESS("press", Comparator.comparing(Book::getPress)),
        PUBLISH_YEAR("publish_year", Comparator.comparingInt(Book::getPublishYear)),
        AUTHOR("author", Comparator.comparing(Book::getAuthor)),
        PRICE("price", Comparator.comparingDouble(Book::getPrice)),
        STOCK("stock", Comparator.comparingInt(Book::getStock));

        private final String value;
        private final Comparator<Book> comparator;

        public String getValue() {
            return value;
        }

        public Comparator<Book> getComparator() {
            return comparator;
        }

        SortColumn(String value, Comparator<Book> comparator) {
            this.value = value;
            this.comparator = comparator;
        }

        public static SortColumn random() {
            return values()[new Random().nextInt(values().length)];
        }
    }

    public Book() {
    }

    public Book(String category, String title, String press, int publishYear,
                String author, double price, int stock) {
        this.category = category;
        this.title = title;
        this.press = press;
        this.publishYear = publishYear;
        this.author = author;
        this.price = price;
        this.stock = stock;
    }

    @Override
    public Book clone() {
        Book b = new Book(category, title, press, publishYear, author, price, stock);
        b.bookId = bookId;
        return b;
    }

    @Override
    public String toString() {
        return "Book {" + "bookId=" + bookId +
                ", category='" + category + '\'' +
                ", title='" + title + '\'' +
                ", press='" + press + '\'' +
                ", publishYear=" + publishYear +
                ", author='" + author + '\'' +
                ", price=" + String.format("%.2f", price) +
                ", stock=" + stock +
                '}';
    }

    /* we assume that two books are equal iff their category...author are equal */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Book book = (Book) o;
        return publishYear == book.publishYear &&
                category.equals(book.category) &&
                title.equals(book.title) &&
                press.equals(book.press) &&
                author.equals(book.author);
    }

    @Override
    public int hashCode() {
        return Objects.hash(category, title, press, publishYear, author);
    }

    public int getBookId() {
        return bookId;
    }

    public void setBookId(int bookId) {
        this.bookId = bookId;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getPress() {
        return press;
    }

    public void setPress(String press) {
        this.press = press;
    }

    public int getPublishYear() {
        return publishYear;
    }

    public void setPublishYear(int publishYear) {
        this.publishYear = publishYear;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getStock() {
        return stock;
    }

    public void setStock(int stock) {
        this.stock = stock;
    }
    public void infoOutput() {
        String bookIdString = String.valueOf(bookId);
        String categoryString = category;
        String titleString = title;
        String pressString = press;
        String publishYearString = String.valueOf(publishYear);
        String authorString = author;
        String priceString = String.valueOf(price);
        String stockString = String.valueOf(stock);

        int bookIdLine = (bookIdString.length() - 1) / 6 + 1;
        int categoryLine = (categoryString.length() - 1) / 8 + 1;
        int titleLine = (titleString.length() - 1) / 9 + 1;
        int pressLine = (pressString.length() - 1) / 7 + 1;
        int publishYearLine = (publishYearString.length() - 1) / 4 + 1;
        int authorLine = (authorString.length() - 1) / 6 + 1;
        int priceLine = (priceString.length() - 1) / 5 + 1;
        int stockLine = (stockString.length() - 1) / 5 + 1;
        int maxLine = Math.max(Math.max(Math.max(Math.max(Math.max(Math.max(bookIdLine, categoryLine), titleLine), pressLine), publishYearLine), authorLine), Math.max(priceLine, stockLine));
        
        String format = "| %6s | %8s | %9s | %7s | %4s | %6s | %5s | %5s |";
        for (int i = 0; i < maxLine; i++) {
            System.out.println(String.format(format,
                    i * 6 < bookIdString.length() ? bookIdString.substring(i * 6, Math.min(bookIdString.length(), (i + 1) * 6)) : "",
                    i * 8 < categoryString.length() ? categoryString.substring(i * 8, Math.min(categoryString.length(), (i + 1) * 8)) : "",
                    i * 9 < titleString.length() ? titleString.substring(i * 9, Math.min(titleString.length(), (i + 1) * 9)) : "",
                    i * 7 < pressString.length() ? pressString.substring(i * 7, Math.min(pressString.length(), (i + 1) * 7)) : "",
                    i * 4 < publishYearString.length() ? publishYearString.substring(i * 4, Math.min(publishYearString.length(), (i + 1) * 4)) : "",
                    i * 6 < authorString.length() ? authorString.substring(i * 6, Math.min(authorString.length(), (i + 1) * 6)) : "",
                    i * 5 < priceString.length() ? priceString.substring(i * 5, Math.min(priceString.length(), (i + 1) * 5)) : "",
                    i * 5 < stockString.length() ? stockString.substring(i * 5, Math.min(stockString.length(), (i + 1) * 5)) : "")
            );
        }
    }
}
