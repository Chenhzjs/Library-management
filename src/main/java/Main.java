import entities.Book;
import entities.Borrow;
import entities.Card;
import entities.Card.CardType;
import queries.*;
import queries.BorrowHistories.Item;
import utils.ConnectConfig;
import utils.DatabaseConnector;

import java.util.logging.Logger;

import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;

public class Main {

    private static final Logger log = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        try {
            // parse connection config from "resources/application.yaml"
            ConnectConfig conf = new ConnectConfig();
            System.out.println(conf.toString());
            log.info("Success to parse connect config.");
            // connect to database
            DatabaseConnector connector = new DatabaseConnector(conf);
            boolean connStatus = connector.connect();
            if (!connStatus) {
                log.severe("Failed to connect database.");
                System.exit(1);
            }
            /* do somethings */

            LibraryManagementSystem library = new LibraryManagementSystemImpl(connector);
            Scanner scanner = new Scanner(System.in);
            
            System.out.println("Successfully create library management system.");
            System.out.println("Please input function you want to use (0-12):");
            System.out.println("1. Store book\t2. Increase stock\t3. Modify book info\t4. Batch store book");
            System.out.println("5. Register card\t6. List all cards\t7. Borrow book\t8. Return book");
            System.out.println("9. List borrow history\t10. Query book\t11. Remove book\t\t12. Remove card");
            System.out.println("0. Exit");
            System.out.print("Your choice: ");
            int choice = scanner.nextInt();
            if (choice < 0 || choice > 12) {
                System.out.println("Invalid choice. Exit.");
                choice = 0;
            }
            while (choice != 0) {
                switch (choice) {
                    case 1:
                        System.out.println("# Store book #");
                        try {
                            System.out.println("Please input book info according to the following instructions:");
                            System.out.println("Book category: ");
                            String bookCategory = System.console().readLine();
                            System.out.println("Book title: ");
                            String bookTitle = System.console().readLine();
                            System.out.println("Book publisher: ");
                            String bookPress = System.console().readLine();
                            System.out.println("Book year: ");
                            int bookPubYear = scanner.nextInt();
                            System.out.println("Book author: ");
                            String bookAuthor = System.console().readLine();
                            System.out.println("Book price: ");
                            double bookPrice = scanner.nextDouble();
                            System.out.println("Book stock: ");
                            int bookStock = scanner.nextInt();
                            Book book = new Book(bookCategory, bookTitle, bookPress, bookPubYear, bookAuthor, bookPrice, bookStock);
                            ApiResult result = library.storeBook(book);
                            if (result.ok) {
                                System.out.println("Successfully store book.");
                            } else {
                                throw new Exception(result.message);
                            }
                        } catch (Exception e) {
                            System.out.println("Failed to store book.");
                            System.out.println(e.getMessage());
                        }
                        break;
                    case 2:
                        System.out.println("# Increase stock #");
                        try {
                            System.out.println("Please input book info according to the following instructions:");
                            System.out.println("(If you don't know the book ID, you can use function 10 to query book.)");
                            System.out.println("Book ID: ");
                            int bookId = scanner.nextInt();
                            System.out.println("Book stock: ");
                            int deltaStock = scanner.nextInt();
                            ApiResult result = library.incBookStock(bookId, deltaStock);
                            if (result.ok) {
                                System.out.println("Successfully increase stock.");
                            } else {
                                throw new Exception(result.message);
                            }
                        } catch (Exception e) {
                            System.out.println("Failed to increase stock.");
                            System.out.println(e.getMessage());
                        }
                        break;
                    case 3:
                        System.out.println("# Modify book info #");
                        try {
                            System.out.println("Please input book info according to the following instructions:");
                            System.out.println("(If you don't know the book ID, you can use function 10 to query book. And Stock will not be modified.)");
                            System.out.println("Book ID: ");
                            int bookId = scanner.nextInt();
                            System.out.println("Book category: ");
                            String bookCategory = System.console().readLine();
                            System.out.println("Book title: ");
                            String bookTitle = System.console().readLine();
                            System.out.println("Book publisher: ");
                            String bookPress = System.console().readLine();
                            System.out.println("Book year: ");
                            int bookPubYear = scanner.nextInt();
                            System.out.println("Book author: ");
                            String bookAuthor = System.console().readLine();
                            System.out.println("Book price: ");
                            int bookPrice = scanner.nextInt();
                            Book book = new Book(bookCategory, bookTitle, bookPress, bookPubYear, bookAuthor, bookPrice, 0);
                            book.setBookId(bookId);
                            ApiResult result = library.modifyBookInfo(book);
                            if (result.ok) {
                                System.out.println("Successfully modify book info.");
                            } else {
                                throw new Exception(result.message);
                            }
                        } catch (Exception e) {
                            System.out.println("Failed to modify book info.");
                            System.out.println(e.getMessage());
                        }
                        break;
                    case 4:
                        System.out.println("# Batch store book #");
                        List<Book> bookList = new ArrayList<>();
                        try {
                            System.out.println("File path: ");
                            System.out.println("(In the text file, each line represents a book. The format is: category, title, publisher, year, author, price, stock)");
                            String filePath = System.console().readLine();
                            // open the file and build a list of books
                            bookList = new ArrayList<>();
                            BufferedReader reader = new BufferedReader(new FileReader(filePath));
                            String line;
                            while ((line = reader.readLine()) != null) {
                                if (line.isEmpty()) {
                                    continue;
                                }
                                String[] bookInfo = line.split(",");
                                Book book = new Book(bookInfo[0], bookInfo[1], bookInfo[2], Integer.parseInt(bookInfo[3]), bookInfo[4], Double.parseDouble(bookInfo[5]), Integer.parseInt(bookInfo[6]));
                                bookList.add(book);
                            }
                            reader.close();
                        } catch (Exception e) {
                            System.out.println("Failed to read file.");
                            System.out.println(e.getMessage());
                        }
                        // store books
                        try {
                            ApiResult result = library.storeBook(bookList);
                            if (result.ok) {
                                System.out.println("Successfully store books.");
                            } else {
                                throw new Exception(result.message);
                            }
                        } catch (Exception e) {
                            System.out.println("Failed to store books.");
                            System.out.println(e.getMessage());
                        }
                        break;
                    case 5:
                        System.out.println("# Register card #");
                        try {
                            System.out.println("Please input card info according to the following instructions:");
                            System.out.println("Card name: ");
                            String cardName = System.console().readLine();
                            System.out.println("Card type (\"S\"/\"T\"): ");
                            String cardType = System.console().readLine();
                            System.out.println("Card department: ");
                            String cardDepartment = System.console().readLine();
                            CardType turnedType = CardType.values(cardType);
                            Card card = new Card(0, cardName, cardDepartment, turnedType);
                            // load card info from payload
                            ApiResult result = library.registerCard(card);
                            if (result.ok) {
                                card = (Card) result.payload;
                                System.out.println("Successfully register card. And the info is:");
                                /*
                                 * ouput like this:
                                 * +---------+-----------+-----------+-----------------+
                                 * | Card ID | Card name | Card type | Card department |
                                 * +---------+-----------+-----------+-----------------+
                                 * | 1       | Tom       | S         | CS              |
                                 * +---------+-----------+-----------+-----------------+
                                 */
                                System.out.println("+---------+-----------+-----------+-----------------+");
                                System.out.println("| Card ID | Card name | Card type | Card department |");
                                System.out.println("+---------+-----------+-----------+-----------------+");
                                card.infoOutput();
                                System.out.println("+---------+-----------+-----------+-----------------+");
                            } else {
                                throw new Exception(result.message);
                            }
                        } catch (Exception e) {
                            System.out.println("Failed to register card.");
                            System.out.println(e.getMessage());
                        }
                        break;
                    case 6:
                        System.out.println("# List all cards #");
                        try {
                            // load card info from payload
                            ApiResult result = library.showCards();
                            CardList cardList = (CardList) result.payload;
                            List<Card> cards = cardList.getCards();
                            if (result.ok) {
                                System.out.println("Successfully list all cards. And the info is:");
                                /*
                                 * ouput like this:
                                 * +---------+-----------+-----------+-----------------+
                                 * | Card ID | Card name | Card type | Card department |
                                 * +---------+-----------+-----------+-----------------+
                                 * | 1       | Tom       | S         | CS              |
                                 * +---------+-----------+-----------+-----------------+
                                 */
                                System.out.println("+---------+-----------+-----------+-----------------+");
                                System.out.println("| Card ID | Card name | Card type | Card department |");
                                System.out.println("+---------+-----------+-----------+-----------------+");
                                for (Card c : cards) {
                                    c.infoOutput();
                                    System.out.println("+---------+-----------+-----------+-----------------+");
                                }
                            } else {
                                throw new Exception(result.message);
                            }
                        } catch (Exception e) {
                            System.out.println("Failed to list all cards.");
                            System.out.println(e.getMessage());
                        }
                        break;
                    case 7:
                        System.out.println("# Borrow book #");
                        try {
                            System.out.println("Please input the borrow info according to the following instructions.");
                            System.out.println("Card ID: ");
                            int cardID = scanner.nextInt();
                            System.out.println("Book ID: ");
                            int bookID = scanner.nextInt();
                            Borrow borrow = new Borrow(bookID, cardID);
                            borrow.resetBorrowTime();
                            ApiResult result = library.borrowBook(borrow);
                            if (result.ok) {
                                System.out.println("Successfully borrow the book.");
                            } else {
                                throw new Exception(result.message);
                            }
                        } catch (Exception e) {
                            System.out.println("Failed to borrow books.");
                            System.out.println(e.getMessage());
                        }
                        break;
                    case 8:
                        System.out.println("# Return book #");
                        try {
                            System.out.println("Please input the borrow info according to the following instructions.");
                            System.out.println("Card ID: ");
                            int cardID = scanner.nextInt();
                            System.out.println("Book ID: ");
                            int bookID = scanner.nextInt();
                            Borrow borrow = new Borrow(bookID, cardID);
                            borrow.resetReturnTime();
                            ApiResult result = library.returnBook(borrow);
                            if (result.ok) {
                                System.out.println("Successfully return the book.");
                            } else {
                                throw new Exception(result.message);
                            }
                        } catch (Exception e) {
                            System.out.println("Failed to return books.");
                            System.out.println(e.getMessage());
                        }
                        break;
                    case 9:
                        System.out.println("# List borrow history #");
                        try {
                            System.out.println("Please input the Card ID to list borrow history.");
                            System.out.println("Card ID: ");
                            int cardID = scanner.nextInt();
                            ApiResult result = library.showBorrowHistory(cardID);
                            if (result.ok) {
                                BorrowHistories histories = (BorrowHistories) result.payload;
                                List<Item> items = histories.getItems();
                                System.out.println("Successfully list borrow history.");
                                /*
                                 * ouput like this:
                                 * +--------+----------+-----------+---------+------+--------+-------+-------------+-------------+2023-04-24
                                 * | BookID | Catagory |   Title   |  Press  | Year | Author | Price | Borrow Time | Return Time |
                                 * +--------+----------+-----------+---------+------+--------+-------+-------------+-------------+
                                 */
                                System.out.println("+--------+----------+-----------+---------+------+--------+-------+-------------+-------------+");
                                System.out.println("| BookID | Catagory |   Title   |  Press  | Year | Author | Price | Borrow Time | Return Time |");
                                System.out.println("+--------+----------+-----------+---------+------+--------+-------+-------------+-------------+");
                                for (Item i : items) {
                                    // i.infoOutput();
                                    System.out.println("+--------+----------+-----------+---------+------+--------+-------+-------------+-------------+");
                                }
                            } else {
                                throw new Exception(result.message);
                            }
                        } catch (Exception e) {
                            System.out.println("Failed to list borrow history.");
                            System.out.println(e.getMessage());
                        }
                        break;
                    case 10:
                        System.out.println("# Query book #");
                        try {
                            System.out.println("Please input query conditions according to the following instructions (if some instruction is not needed, just press enter).");

                            System.out.println("Book Category: ");
                            String category = System.console().readLine();

                            System.out.println("Book Title: ");
                            String title = System.console().readLine();

                            System.out.println("Book Publisher: ");
                            String publisher = System.console().readLine();

                            System.out.println("Min publish year: ");
                            String minYear = System.console().readLine();

                            System.out.println("Max publish year: ");
                            String maxYear = System.console().readLine();

                            System.out.println("Book Author: ");
                            String author = System.console().readLine();

                            System.out.println("Min price: ");
                            String minPrice = System.console().readLine();

                            System.out.println("Max price: ");
                            String maxPrice = System.console().readLine();

                            System.out.println("Sort by: ");
                            System.out.println("1. Book ID");
                            System.out.println("2. Book Category");
                            System.out.println("3. Book Title");
                            System.out.println("4. Book Publisher");
                            System.out.println("5. Book Publish Year");
                            System.out.println("6. Book Author");
                            System.out.println("7. Book Price");
                            System.out.println("8. Book Stock");
                            System.out.println("Your choice: (default 1)");
                            String sortString = System.console().readLine();
                            if (sortString == null || sortString.length() == 0) {
                                sortString = "1";
                            }
                            int sort = Integer.parseInt(sortString);
                            if (sort < 1 || sort > 8) {
                                sort = 1;
                            }

                            System.out.println("Sort order: ");
                            System.out.println("1. Ascending");
                            System.out.println("2. Descending");
                            System.out.println("Your choice: (default 1)");
                            String orderString = System.console().readLine();
                            if (orderString == null || orderString.length() == 0) {
                                orderString = "1";
                            }
                            int order = Integer.parseInt(orderString);
                            if (order < 1 || order > 2) {
                                order = 1;
                            }

                            // prepare query conditions
                            BookQueryConditions conditions = new BookQueryConditions();
                            if (category != null && category.length() != 0) {
                                conditions.setCategory(category);
                            }
                            if (title != null && title.length() != 0) {
                                conditions.setTitle(title);
                            }
                            if (publisher != null && publisher.length() != 0) {
                                conditions.setPress(publisher);
                            }
                            if (minYear != null && minYear.length() != 0) {
                                conditions.setMinPublishYear(Integer.parseInt(minYear));
                            }
                            if (maxYear != null && maxYear.length() != 0) {
                                conditions.setMaxPublishYear(Integer.parseInt(maxYear));
                            }
                            if (author != null && author.length() != 0) {
                                conditions.setAuthor(author);
                            }
                            if (minPrice != null && minPrice.length() != 0) {
                                conditions.setMinPrice(Double.parseDouble(minPrice));
                            }
                            if (maxPrice != null && maxPrice.length() != 0) {
                                conditions.setMaxPrice(Double.parseDouble(maxPrice));
                            }
                            Book.SortColumn sortColumn = Book.SortColumn.values()[sort - 1];
                            SortOrder sortOrder = SortOrder.values()[order - 1];
                            conditions.setSortBy(sortColumn);
                            conditions.setSortOrder(sortOrder);

                            ApiResult result = library.queryBook(conditions);
                            BookQueryResults bookQueryResults = (BookQueryResults) result.payload;
                            List<Book> books = bookQueryResults.getResults();
                            /*
                             * ouput like this:
                             * +--------+----------+-----------+---------+------+--------+-------+-------+
                             * | BookID | Catagory |   Title   |  Press  | Year | Author | Price | Stock |
                             * +--------+----------+-----------+---------+------+--------+-------+-------+
                             */
                            System.out.println("+--------+----------+-----------+---------+------+--------+-------+-------+");
                            System.out.println("| BookID | Catagory |   Title   |  Press  | Year | Author | Price | Stock |");
                            System.out.println("+--------+----------+-----------+---------+------+--------+-------+-------+");
                            for (Book b : books) {
                                b.infoOutput();
                                System.out.println("+--------+----------+-----------+---------+------+--------+-------+-------+");
                            }
                        } catch (Exception e) {
                            System.out.println("Failed to query book.");
                            System.out.println(e.getMessage());
                        }
                        break;
                    case 11:
                        System.out.println("# Remove book #");
                        try {
                            System.out.println("Please input book ID of the book you want to remove:");
                            int bookId = scanner.nextInt();
                            ApiResult result = library.removeBook(bookId);
                            if (result.ok) {
                                System.out.println("Success to remove book.");
                            } else {
                                System.out.println("Failed to remove book.");
                                System.out.println(result.message);
                            }
                        } catch (Exception e) {
                            System.out.println("Failed to remove book.");
                            System.out.println(e.getMessage());
                        }
                        break;
                    case 12:
                        System.out.println("# Remove card #");
                        try {
                            System.out.println("Please input card ID of the card you want to remove:");
                            int cardId = scanner.nextInt();
                            ApiResult result = library.removeCard(cardId);
                            if (result.ok) {
                                System.out.println("Success to remove card.");
                            } else {
                                System.out.println("Failed to remove card.");
                                System.out.println(result.message);
                            }
                        } catch (Exception e) {
                            System.out.println("Failed to remove card.");
                            System.out.println(e.getMessage());
                        }
                        break;
                    default:
                        System.out.println("Invalid choice. Exit.");
                        choice = 0;
                        break;
                }
                System.out.println("Operation end.");
                System.out.println("Please input function you want to use (0-12):");
                System.out.println("1. Store book\t2. Increase stock\t3. Modify book info\t4. Batch store book");
                System.out.println("5. Register card\t6. List all cards\t7. Borrow book\t8. Return book");
                System.out.println("9. List borrow history\t10. Query book\t11. Remove book\t\t12. Remove card");
                System.out.println("0. Exit");
                System.out.print("Your choice: ");
                choice = scanner.nextInt();
            }

            // release database connection handler
            if (connector.release()) {
                log.info("Success to release connection.");
            } else {
                log.warning("Failed to release connection.");
            }

            scanner.close();

            System.out.println("Bye.");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}