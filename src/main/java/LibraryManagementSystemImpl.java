import entities.Book;
import entities.Borrow;
import entities.Card;
import queries.*;
import utils.DBInitializer;
import utils.DatabaseConnector;
import queries.BorrowHistories.Item;
import java.util.ArrayList;
import java.sql.*;
import java.util.List;

public class LibraryManagementSystemImpl implements LibraryManagementSystem {

    private final DatabaseConnector connector;

    public LibraryManagementSystemImpl(DatabaseConnector connector) {
        this.connector = connector;
    }

    @Override
    public ApiResult storeBook(Book book) {
        Connection conn = connector.getConn();
        // conn.setAutoCommit(false);
        try {
            PreparedStatement checkStmt = conn.prepareStatement("SELECT COUNT(*) FROM book WHERE category = ? AND title = ? AND press = ? AND publish_year = ? AND author = ?");
            checkStmt.setString(1, book.getCategory());
            checkStmt.setString(2, book.getTitle());
            checkStmt.setString(3, book.getPress());
            checkStmt.setInt(4, book.getPublishYear());
            checkStmt.setString(5, book.getAuthor());
            ResultSet checkRs = checkStmt.executeQuery();
            if (checkRs.next()) {
                int count = checkRs.getInt(1);
                if (count > 0) {
                    throw new Exception("Book already exists");
                }
            }
            // System.out.println("Check finished");
            // PreparedStatement idStmt = conn.prepareStatement("SELECT COUNT(*), MAX(book_id) FROM book");
            // ResultSet idRs = idStmt.executeQuery();
            // if (idRs.next()) {
            //     int count = idRs.getInt(1);
            //     int max_id;
            //     if (count == 0) max_id = -1;
            //     else {
            //         max_id = idRs.getInt(2);
            //     }
            //     book.setBookId(max_id + 1);
            // }
            // System.out.println("success before insert");
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO book (category, title, press, publish_year, author, price, stock) VALUES (?, ?, ?, ?, ?, ? ,?)");
            // stmt.setInt(1, book.getBookId());
            stmt.setString(1, book.getCategory());
            stmt.setString(2, book.getTitle());
            stmt.setString(3, book.getPress());
            stmt.setInt(4, book.getPublishYear());
            stmt.setString(5, book.getAuthor());
            stmt.setDouble(6, book.getPrice());
            stmt.setInt(7, book.getStock());

            // System.out.println("success before update");
            stmt.executeUpdate();

            PreparedStatement bookidStmt = conn.prepareStatement("SELECT book_id FROM book WHERE category = ? AND title = ? AND press = ? AND publish_year = ? AND author = ? AND price = ? AND stock = ?");
            bookidStmt.setString(1, book.getCategory());
            bookidStmt.setString(2, book.getTitle());
            bookidStmt.setString(3, book.getPress());
            bookidStmt.setInt(4, book.getPublishYear());
            bookidStmt.setString(5, book.getAuthor());
            bookidStmt.setDouble(6, book.getPrice());
            bookidStmt.setInt(7, book.getStock());
            ResultSet bookidRs = bookidStmt.executeQuery();
            if (bookidRs.next()) {
                book.setBookId(bookidRs.getInt("book_id"));
            } else {
                throw new Exception("Book not found");
            }
            commit(conn);
            return new ApiResult(true, null);
        } catch (Exception e) {
            rollback(conn);
            System.out.println(e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult incBookStock(int bookId, int deltaStock) {
        Connection conn = connector.getConn();
        // conn.setAutoCommit(false);
        try {
            PreparedStatement stockStmt = conn.prepareStatement("SELECT stock FROM book WHERE book_id = ?");
            stockStmt.setInt(1, bookId);
            ResultSet stockRs = stockStmt.executeQuery();
            if (stockRs.next()) {
                int currentStock = stockRs.getInt("stock");
                int newStock = currentStock + deltaStock;
                if (newStock < 0) {
                    throw new Exception("Invalid stock value");
                }
                PreparedStatement updateStmt = conn.prepareStatement("UPDATE book SET stock = ? WHERE book_id = ?");
                updateStmt.setInt(1, newStock);
                updateStmt.setInt(2, bookId);
                updateStmt.executeUpdate();
                commit(conn);
            } else {
                throw new Exception("Book not found");
            }
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult storeBook(List<Book> books) {
        Connection conn = connector.getConn();
        // conn.setAutoCommit(false);
        try {
            PreparedStatement checkStmt = conn.prepareStatement("SELECT COUNT(*) FROM book WHERE category = ? AND title = ? AND press = ? AND publish_year = ? AND author = ?");
            for (Book book : books) {
                checkStmt.setString(1, book.getCategory());
                checkStmt.setString(2, book.getTitle());
                checkStmt.setString(3, book.getPress());
                checkStmt.setInt(4, book.getPublishYear());
                checkStmt.setString(5, book.getAuthor());
                ResultSet checkRs = checkStmt.executeQuery();
                if (checkRs.next()) {
                    int count = checkRs.getInt(1);
                    if (count > 0) {
                        throw new Exception("Book already exists");
                    }
                }
            }
            // PreparedStatement idStmt = conn.prepareStatement("SELECT MAX(book_id) FROM book");
            // ResultSet idRs = idStmt.executeQuery();
            // if (idRs.next()) {
            //     int max_id = idRs.getInt(1);
            //     for (Book book : books) {
            //         book.setBookId(++ max_id);
            //     }
            // }
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO book (category, title, press, publish_year, author, price, stock) VALUES (?, ?, ?, ?, ?, ?, ?)");
            for (Book book : books) {
                // stmt.setInt(1, book.getBookId());
                stmt.setString(1, book.getCategory());
                stmt.setString(2, book.getTitle());
                stmt.setString(3, book.getPress());
                stmt.setInt(4, book.getPublishYear());
                stmt.setString(5, book.getAuthor());
                stmt.setDouble(6, book.getPrice());
                stmt.setInt(7, book.getStock());
                stmt.executeUpdate();
                PreparedStatement bookidStmt = conn.prepareStatement("SELECT book_id FROM book WHERE category = ? AND title = ? AND press = ? AND publish_year = ? AND author = ? AND price = ? AND stock = ?");
                bookidStmt.setString(1, book.getCategory());
                bookidStmt.setString(2, book.getTitle());
                bookidStmt.setString(3, book.getPress());
                bookidStmt.setInt(4, book.getPublishYear());
                bookidStmt.setString(5, book.getAuthor());
                bookidStmt.setDouble(6, book.getPrice());
                bookidStmt.setInt(7, book.getStock());
                ResultSet bookidRs = bookidStmt.executeQuery();
                if (bookidRs.next()) {
                    book.setBookId(bookidRs.getInt("book_id"));
                } else {
                    throw new Exception("Book not found");
                }

            }

            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult removeBook(int bookId) {
        Connection conn = connector.getConn();
        // conn.setAutoCommit(false);
        try{
            PreparedStatement borrowStmt = conn.prepareStatement("SELECT COUNT(*) FROM borrow WHERE book_id = ? AND return_time = 0");
            borrowStmt.setInt(1, bookId);
            ResultSet borrowRs = borrowStmt.executeQuery();
            if (borrowRs.next()) {
                int count = borrowRs.getInt(1);
                if (count > 0) {
                    throw new Exception("Cannot remove book. It is currently borrowed by someone.");
                }
            }
            PreparedStatement bookStmt = conn.prepareStatement("SELECT COUNT(*) FROM book WHERE book_id = ?");
            bookStmt.setInt(1, bookId);
            ResultSet bookRs = bookStmt.executeQuery();
            if (bookRs.next()) {
                int count = bookRs.getInt(1);
                if (count == 0) {
                    throw new Exception("No book found");
                }
            }
            PreparedStatement removeStmt = conn.prepareStatement("DELETE FROM book WHERE book_id = ?");
            removeStmt.setInt(1, bookId);
            removeStmt.executeUpdate();
            commit(conn);
            return new ApiResult(true, null);
        } catch (Exception e) {
            rollback(conn);
            System.out.println(e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        
    }

    @Override
    public ApiResult modifyBookInfo(Book book) {
        Connection conn = connector.getConn();
        // conn.setAutoCommit(false);
        
        try {
            // PreparedStatement checkStockStmt = conn.prepareStatement("SELECT stock FROM book WHERE book_id = ?");
            // checkStockStmt.setInt(1, book.getBookId());
            // ResultSet stockRs = checkStockStmt.executeQuery();
            // if (stockRs.next()) {
            //     int currentStock = stockRs.getInt("stock");
            //     if (currentStock != book.getStock()) {
            //         String mess = "Stock does not match" + "currentStock = " + currentStock + "bookstock = " + book.getStock();
            //         throw new Exception(mess);
            //     }
            // } else {
            //     throw new Exception("Book not found");
            // }
            PreparedStatement updateStmt = conn.prepareStatement("UPDATE book SET category = ?, title = ?, press = ?, publish_year = ?, author = ?, price = ? WHERE book_id = ?");
            updateStmt.setString(1, book.getCategory());
            updateStmt.setString(2, book.getTitle());
            updateStmt.setString(3, book.getPress());
            updateStmt.setInt(4, book.getPublishYear());
            updateStmt.setString(5, book.getAuthor());
            updateStmt.setDouble(6, book.getPrice());
            updateStmt.setInt(7, book.getBookId());
            updateStmt.executeUpdate();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            System.out.println(e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult queryBook(BookQueryConditions conditions) {
        Connection conn = connector.getConn();
        // conn.setAutoCommit(false);
        try {
            String query = "SELECT * FROM book WHERE";

            if (conditions.getCategory() != null) {
                query += " category = ? AND ";
            }
            if (conditions.getTitle() != null) {
                query += " title LIKE ? AND ";
            }
            if (conditions.getPress() != null) {
                query += " press LIKE ? AND ";
            }
            if (conditions.getMinPublishYear() != null) {
                query += " publish_year >= ? AND ";
            }
            if (conditions.getMaxPublishYear() != null) {
                query += " publish_year <= ? AND ";
            }
            if (conditions.getAuthor() != null) {
                query += " author LIKE ? AND ";
            }
            if (conditions.getMinPrice() != null) {
                query += " price >= ? AND ";
            }
            if (conditions.getMaxPrice() != null) {
                query += " price <= ? AND ";
            }
            
            query = query.substring(0, query.length() - 5);
            query += " ORDER BY " + conditions.getSortBy().getValue() + " " + conditions.getSortOrder().getValue();
            if (conditions.getSortBy() != Book.SortColumn.BOOK_ID) {
                query += ", book_id asc";
            }
            // System.out.println(query);
            PreparedStatement stmt = conn.prepareStatement(query);
            int index = 1;
            if (conditions.getCategory() != null) stmt.setString(index ++, conditions.getCategory());
            if (conditions.getTitle() != null) stmt.setString(index ++, "%" + conditions.getTitle() + "%");
            if (conditions.getPress() != null) stmt.setString(index ++, "%" + conditions.getPress() + "%");
            if (conditions.getMinPublishYear() != null) stmt.setInt(index ++, conditions.getMinPublishYear());
            if (conditions.getMaxPublishYear() != null) stmt.setInt(index ++, conditions.getMaxPublishYear());
            if (conditions.getAuthor() != null) stmt.setString(index ++, "%" + conditions.getAuthor() + "%");
            if (conditions.getMinPrice() != null) stmt.setDouble(index ++, conditions.getMinPrice());
            if (conditions.getMaxPrice() != null) stmt.setDouble(index ++, conditions.getMaxPrice());
            ResultSet rs = stmt.executeQuery();
            // System.out.println("end query");
            List<Book> books = new ArrayList<Book>();
            while (rs.next()) {
                Book book = new Book();
                book.setBookId(rs.getInt("book_id"));
                book.setCategory(rs.getString("category"));
                book.setTitle(rs.getString("title"));
                book.setPress(rs.getString("press"));
                book.setPublishYear(rs.getInt("publish_year"));
                book.setAuthor(rs.getString("author"));
                book.setPrice(rs.getDouble("price"));
                book.setStock(rs.getInt("stock"));
                books.add(book);
            } 
            commit(conn);
            return new ApiResult(true, new BookQueryResults(books));
        } catch (Exception e) {
            rollback(conn);
            System.out.println(e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult borrowBook(Borrow borrow) {
        Connection conn = connector.getConn();
        // conn.setAutoCommit(false);
        try {
            PreparedStatement checkBookStmt = conn.prepareStatement("SELECT COUNT(*) FROM book(updlock) WHERE book_id = ?");
            checkBookStmt.setInt(1, borrow.getBookId());
            ResultSet bookRs = checkBookStmt.executeQuery();
            if (bookRs.next()) {
                int count = bookRs.getInt(1);
                if(count == 0) {
                    throw new Exception("Book not found");
                }

            }
            PreparedStatement checkCardStmt = conn.prepareStatement("SELECT COUNT(*) FROM card WHERE card_id = ?");
            checkCardStmt.setInt(1, borrow.getCardId());
            ResultSet cardRs = checkCardStmt.executeQuery();
            if (cardRs.next()) {
                int count = cardRs.getInt(1);
                if (count == 0) {
                    throw new Exception("Card not found");
                }
            }

            PreparedStatement checkStockStmt = conn.prepareStatement("SELECT stock FROM book(updlock) WHERE book_id = ?");
            checkStockStmt.setInt(1, borrow.getBookId());
            ResultSet stockRs = checkStockStmt.executeQuery();
            int currentStock = -1;
            if (stockRs.next()) {
                currentStock = stockRs.getInt(1);
                // String mess = "" + currentStock;
                // System.out.println(mess);
                if (currentStock == 0) {
                    throw new Exception("Stock does not match");
                }
            }
            PreparedStatement checkBorrowStmt = conn.prepareStatement("SELECT COUNT(*) FROM borrow WHERE book_id = ? AND card_id = ? AND return_time = 0");
            checkBorrowStmt.setInt(1, borrow.getBookId());
            checkBorrowStmt.setInt(2, borrow.getCardId());
            ResultSet borrowRs = checkBorrowStmt.executeQuery();
            if (borrowRs.next()) {
                int count = borrowRs.getInt(1);
                if (count > 0) {
                    throw new Exception("The book has already been borrowed by the user.");
                }
            }
            PreparedStatement borrowStmt = conn.prepareStatement("INSERT INTO borrow (book_id, card_id, borrow_time) VALUES (?, ?, ?)");
            borrowStmt.setInt(1, borrow.getBookId());
            borrowStmt.setInt(2, borrow.getCardId());
            borrowStmt.setLong(3, borrow.getBorrowTime());
            borrowStmt.executeUpdate();

            PreparedStatement updateStockStmt = conn.prepareStatement("UPDATE book SET stock = stock - 1 WHERE book_id = ?");
            updateStockStmt.setInt(1, borrow.getBookId());
            updateStockStmt.executeUpdate();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            // System.out.println(e.getMessage());
            // System.out.println("borrow");
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult returnBook(Borrow borrow) {
        Connection conn = connector.getConn();
        // conn.setAutoCommit(false);
        try {
            PreparedStatement checkBorrowStmt = conn.prepareStatement("SELECT COUNT(*) FROM borrow WHERE book_id = ? AND card_id = ? AND borrow_time = ?");
            checkBorrowStmt.setInt(1, borrow.getBookId());
            checkBorrowStmt.setInt(2, borrow.getCardId());
            checkBorrowStmt.setLong(3, borrow.getBorrowTime());
            ResultSet borrowRs = checkBorrowStmt.executeQuery();
            if (borrowRs.next()) {
                int count = borrowRs.getInt(1);
                if (count == 0) {
                    throw new Exception("Borrow record not found");
                }
            } else {
                throw new Exception("Borrow record not found");
            }

            PreparedStatement returnStmt = conn.prepareStatement("UPDATE borrow SET return_time = ? WHERE book_id = ? AND card_id = ? AND borrow_time = ?");
            returnStmt.setLong(1, borrow.getReturnTime());
            returnStmt.setInt(2, borrow.getBookId());
            returnStmt.setInt(3, borrow.getCardId());
            returnStmt.setLong(4, borrow.getBorrowTime());
            returnStmt.executeUpdate();

            PreparedStatement updateStockStmt = conn.prepareStatement("UPDATE book SET stock = stock + 1 WHERE book_id = ?");
            updateStockStmt.setInt(1, borrow.getBookId());
            updateStockStmt.executeUpdate();
            commit(conn);
            return new ApiResult(true, null);
        } catch (Exception e) {
            rollback(conn);
            // System.out.println(e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        
    }

    @Override
    public ApiResult showBorrowHistory(int cardId) {
        Connection conn = connector.getConn();
        // conn.setAutoCommit(false);
        try {
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM borrow WHERE card_id = ? ORDER BY borrow_time DESC, book_id ASC");
            PreparedStatement bookStmt = conn.prepareStatement("SELECT * FROM book WHERE book_id = ?");
            stmt.setInt(1, cardId);
            ResultSet rs = stmt.executeQuery();
            List<Item> borrows = new ArrayList<Item>();
            while (rs.next()) {
                Borrow borrow = new Borrow();
                borrow.setBookId(rs.getInt("book_id"));
                borrow.setCardId(rs.getInt("card_id"));
                borrow.setBorrowTime(rs.getLong("borrow_time"));
                borrow.setReturnTime(rs.getLong("return_time"));
                bookStmt.setInt(1, borrow.getBookId());
                ResultSet bookRs = bookStmt.executeQuery();
                if (bookRs.next()) {
                    Book book = new Book();
                    book.setBookId(bookRs.getInt("book_id"));
                    book.setCategory(bookRs.getString("category"));
                    book.setTitle(bookRs.getString("title"));
                    book.setPress(bookRs.getString("press"));
                    book.setPublishYear(bookRs.getInt("publish_year"));
                    book.setAuthor(bookRs.getString("author"));
                    book.setPrice(bookRs.getDouble("price"));
                    Item item = new Item(cardId, book, borrow);
                    borrows.add(item);
                } else {
                    throw new Exception("Book not found");
                }
            }
            commit(conn);
            return new ApiResult(true, new BorrowHistories(borrows));
        } catch (Exception e) {
            rollback(conn);
            System.out.println(e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        // return new ApiResult(true, null);
    }

    @Override
    public ApiResult registerCard(Card card) {
        Connection conn = connector.getConn();
        // conn.setAutoCommit(false);
        try {
            PreparedStatement checkCardStmt = conn.prepareStatement("SELECT COUNT(*) FROM card WHERE name = ? AND department = ? AND type = ?");
            checkCardStmt.setString(1, card.getName());
            checkCardStmt.setString(2, card.getDepartment());
            checkCardStmt.setString(3, card.getType().getStr());
            ResultSet cardRs = checkCardStmt.executeQuery();
            if (cardRs.next()) {
                int count = cardRs.getInt(1);
                if (count > 0) {
                    throw new Exception("Card already exists");
                }
            }
            // System.out.println("Check finished");
            PreparedStatement registerStmt = conn.prepareStatement("INSERT INTO card (name, department, type) VALUES (?, ?, ?)");
            // registerStmt.setInt(1, cardId);
            registerStmt.setString(1, card.getName());
            registerStmt.setString(2, card.getDepartment());
            registerStmt.setString(3, card.getType().getStr());
            registerStmt.executeUpdate();
            // System.out.println("success insert");

            PreparedStatement cardidStmt = conn.prepareStatement("SELECT card_id FROM card WHERE name = ? AND department = ? AND type = ?");
            
            
            cardidStmt.setString(1, card.getName());
            cardidStmt.setString(2, card.getDepartment());
            cardidStmt.setString(3, card.getType().getStr());
            ResultSet cardidRs = cardidStmt.executeQuery();
            if (cardidRs.next()) {
                card.setCardId(cardidRs.getInt("card_id"));
            } else {
                throw new Exception("Card not found");
            }
            // System.out.println("success select");
            commit(conn);
            return new ApiResult(true, null);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        
    }

    @Override
    public ApiResult removeCard(int cardId) {
        Connection conn = connector.getConn();
        // conn.setAutoCommit(false);
        try {
            PreparedStatement checkBorrowedBooksStmt = conn.prepareStatement("SELECT COUNT(*) FROM borrow WHERE card_id = ? AND return_time = 0");
            checkBorrowedBooksStmt.setInt(1, cardId);
            ResultSet borrowedBooksRs = checkBorrowedBooksStmt.executeQuery();
            if (borrowedBooksRs.next()) {
                int count = borrowedBooksRs.getInt(1);
                if (count > 0) {
                    throw new Exception("Cannot remove card. There are still borrowed books associated with this card.");
                }
            }
            PreparedStatement checkCardStmt = conn.prepareStatement("SELECT COUNT(*) FROM card WHERE card_id = ?");
            checkCardStmt.setInt(1, cardId);
            ResultSet cardRs = checkCardStmt.executeQuery();
            if (cardRs.next()) {
                int count = cardRs.getInt(1);
                if (count == 0) {
                    throw new Exception("Card does not exist.");
                }
                
            }
            PreparedStatement removeCardStmt = conn.prepareStatement("DELETE FROM card WHERE card_id = ?");
            removeCardStmt.setInt(1, cardId);
            removeCardStmt.executeUpdate();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            // System.out.println(e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    @Override
    public ApiResult showCards() {
        Connection conn = connector.getConn();
        // conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            String query = "SELECT * FROM card ORDER BY card_id asc";
            ResultSet rs = stmt.executeQuery(query);
            List<Card> cards = new ArrayList<Card>();
            while (rs.next()) {
                Card card = new Card();

                card.setCardId(rs.getInt("card_id"));
                card.setName(rs.getString("name"));
                card.setDepartment(rs.getString("department"));

                String str = rs.getString("type");

                card.setTypeStr(str);
                // System.out.println("looping");

                // String record = card.getCardId() + " " + card.getName() + " " + card.getDepartment() + " " + card.getType().getStr();
                // System.out.println(record); 
                cards.add(card);
            }
            // System.out.println("success before commit");
            commit(conn);
            return new ApiResult(true, new CardList(cards));
        } catch (Exception e) {
            rollback(conn);
            System.out.println(e.getMessage());
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult resetDatabase() {
        Connection conn = connector.getConn();
        // conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            DBInitializer initializer = connector.getConf().getType().getDbInitializer();
            stmt.addBatch(initializer.sqlDropBorrow());
            stmt.addBatch(initializer.sqlDropBook());
            stmt.addBatch(initializer.sqlDropCard());
            stmt.addBatch(initializer.sqlCreateCard());
            stmt.addBatch(initializer.sqlCreateBook());
            stmt.addBatch(initializer.sqlCreateBorrow());
            stmt.executeBatch();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    private void rollback(Connection conn) {
        try {
            conn.rollback();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void commit(Connection conn) {
        try {
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
