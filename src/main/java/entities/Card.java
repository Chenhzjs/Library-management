package entities;

import java.util.Objects;
import java.util.Random;

public final class Card {

    public enum CardType {
        Student("S"),
        Teacher("T");

        public String str;

        CardType(String str) {
            this.str = str;
        }

        public String getStr() {
            return str;
        }
        public void setStr(String str) {
            this.str = str;
        }
        public static CardType values(String s) {
            if ("S".equals(s)) {
                return Student;
            } else if ("T".equals(s)) {
                return Teacher;
            } else {
                return null;
            }
        }

        public static CardType random() {
            return values()[new Random().nextInt(values().length)];
        }
    };

    private int cardId;
    private String name;
    private String department;
    private CardType type;

    /* we assume that two cards are equal iff their name...type are equal */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Card card = (Card) o;
        return name.equals(card.name) &&
                department.equals(card.department) &&
                type == card.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, department, type);
    }

    @Override
    public String toString() {
        return "Card {" + "cardId=" + cardId +
                ", name='" + name + '\'' +
                ", department='" + department + '\'' +
                ", type=" + type +
                '}';
    }

    public Card() {
    }

    public Card(int cardId, String name, String department, CardType type) {
        this.cardId = cardId;
        this.name = name;
        this.department = department;
        this.type = type;
    }

    @Override
    public Card clone() {
        return new Card(cardId, name, department, type);
    }

    public int getCardId() {
        return cardId;
    }

    public void setCardId(int cardId) {
        this.cardId = cardId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public CardType getType() {
        return type;
    }

    public void setType(CardType type) {
        this.type = type;
    }
    public void setTypeStr(String str) {
        CardType type = CardType.values(str);
        setType(type);
    }
    public void infoOutput() {
        String cardIdStr = String.valueOf(this.getCardId());
        String cardNameStr = this.getName();
        String cardTypeStr = this.getType().toString();
        String cardDepartmentStr = this.getDepartment();
        int idLine = (cardIdStr.length() - 1) / 7 + 1;
        int nameLine = (cardNameStr.length() - 1) / 9 + 1;
        int typeLine = (cardTypeStr.length() - 1) / 9 + 1;
        int departmentLine = (cardDepartmentStr.length() - 1) / 15 + 1;
        int maxLine = Math.max(idLine, Math.max(nameLine, Math.max(typeLine, departmentLine)));
        String format = "| %7s | %9s | %9s | %15s |";
        for (int i = 0; i < maxLine; i++) {
            System.out.println(String.format(format,
                    i * 7 < Math.min((i + 1) * 7, cardIdStr.length()) ? cardIdStr.substring(i * 7, Math.min((i + 1) * 7, cardIdStr.length())) : "",
                    i * 9 < Math.min((i + 1) * 9, cardNameStr.length()) ? cardNameStr.substring(i * 9, Math.min((i + 1) * 9, cardNameStr.length())) : "",
                    i * 9 < Math.min((i + 1) * 9, cardTypeStr.length()) ? cardTypeStr.substring(i * 9, Math.min((i + 1) * 9, cardTypeStr.length())) : "",
                    i * 15 < Math.min((i + 1) * 15, cardDepartmentStr.length()) ? cardDepartmentStr.substring(i * 15, Math.min((i + 1) * 15, cardDepartmentStr.length())) : ""
            ));
        }
    }
}
