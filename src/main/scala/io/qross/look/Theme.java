package io.qross.look;

import io.qross.core.DataRow;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Theme {

    public String primaryColor;
    public String lighterColor;
    public String darkerColor;

    public String getPrimaryColor() {
        return primaryColor;
    }

    public String getLighterColor() {
        return lighterColor;
    }

    public String getDarkerColor() {
        return darkerColor;
    }

    public Theme(String primaryColor, String lighterColor, String darkerColor) {
        this.primaryColor = primaryColor;
        this.lighterColor = lighterColor;
        this.darkerColor = darkerColor;
    }

    public static Theme split(String theme) {
        String[] colors = theme.split(",");
        if (colors.length == 3) {
            return new Theme(colors[0], colors[1], colors[2]);
        }
        else {
            return themes.get(new Random().nextInt(32));
        }
    }

    public String join() {
        return this.primaryColor + "," + this.lighterColor + "," + this.darkerColor;
    }



    public static List<Theme> themes = new ArrayList<>();
    static {

        themes.add(new Theme("#FF8000", "#FFA222", "#DD6000"));
        themes.add(new Theme("#FF4343", "#FF6565", "#DD2121"));
        themes.add(new Theme("#E74856", "#F96A78", "#B52634"));
        themes.add(new Theme("#E81123", "#FA3345", "#B60001"));
        themes.add(new Theme("#E3008C", "#F522AE", "#B1006A"));
        themes.add(new Theme("#BB0000", "#DD2222", "#990000"));
        themes.add(new Theme("#DD7711", "#FF9933", "#BB5500"));
        themes.add(new Theme("#D13438", "#F3565A", "#B01216"));
        themes.add(new Theme("#DA3B01", "#FC5D23", "#B81900"));
        themes.add(new Theme("#DD0060", "#FF0080", "#BB0040"));
        themes.add(new Theme("#C239B3", "#E45BE5", "#A01791"));
        themes.add(new Theme("#C30052", "#F52274", "#A10030"));
        themes.add(new Theme("#B146C2", "#D368E4", "#9024A0"));
        themes.add(new Theme("#BF0077", "#EF2299", "#9D0055"));
        themes.add(new Theme("#907045", "#B19067", "#705023"));
        themes.add(new Theme("#9A0089", "#BC22AB", "#780067"));
        themes.add(new Theme("#6000DD", "#8000FF", "#4000BB"));
        themes.add(new Theme("#40BB00", "#60DD00", "#209900"));
        themes.add(new Theme("#21C57A", "#43C79C", "#00A358"));
        themes.add(new Theme("#22BB22", "#44DD44", "#009900"));
        themes.add(new Theme("#317AB9", "#539CDB", "#105897"));
        themes.add(new Theme("#10893E", "#32AB5F", "#00671B"));
        themes.add(new Theme("#00B294", "#22D4B6", "#009072"));
        themes.add(new Theme("#00AA50", "#00CC70", "#008850"));
        themes.add(new Theme("#00CC6A", "#22EE8C", "#00AA48"));
        themes.add(new Theme("#00B7C3", "#22D9E5", "#0095A1"));
        themes.add(new Theme("#0099BC", "#22BBDE", "#00779A"));
        themes.add(new Theme("#0080FF", "#22A2FF", "#0060DD"));
        themes.add(new Theme("#0078D7", "#229AF9", "#0056B5"));
        themes.add(new Theme("#0063B1", "#2285D3", "#004190"));
        themes.add(new Theme("#038387", "#25A5A9", "#016162"));
        themes.add(new Theme("#018574", "#23A796", "#006352"));
    }

    public static Theme pick() {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes != null) {
            HttpServletRequest request = attributes.getRequest();
            Object theme = request.getSession().getAttribute("theme");
            if (theme == null || theme.toString().isEmpty()) {
                int index = new Random().nextInt(32);
                request.getSession().setAttribute("theme", themes.get(index).join());
                return themes.get(index);
            }
            else {
                return Theme.split(theme.toString());
            }
        }
        else {
            return themes.get(new Random().nextInt(32));
        }
    }

    public static DataRow random() {
        Theme theme = Theme.pick();
        DataRow row = new DataRow();
        row.set("primary", theme.primaryColor);
        row.set("lighter", theme.lighterColor);
        row.set("darker", theme.darkerColor);

        return row;
    }
}
