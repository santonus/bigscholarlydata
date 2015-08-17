package org.dblpmining;
import java.util.*;

/**
 * @author ley
 */
public class Person {
    private static int maxPublCount = 0;
    private static int maxNameLength = 0;
    private static Map personMap = new HashMap(600000);
    private static Set tmpSet = new HashSet(500);
    
    private String name;
    private Set nameParts;
    private int count;
    private int tmp;
    private Publication[] publs;
    private Person[] coauthors;

    public Person(String n) {
        name = n;
        count = 0;
        personMap.put(name,this);
        if (maxNameLength < name.length())
            maxNameLength = name.length();
        nameParts = new HashSet();
        StringTokenizer st = new StringTokenizer(name," -");
        while (st.hasMoreTokens()) {
            nameParts.add(st.nextToken());
        }
    }
    
    public void increment() {
        count++;
        if (count > maxPublCount)
            maxPublCount = count;
    }
    
    public String getName() {
        return name;
    }
    
    public Set getNameParts() {
        return nameParts;
    }
    
    public int getNumberOfCoauthors() {
        return (coauthors == null) ? 0 : coauthors.length;
    }
    
    public Person[] getCoauthors() {
        return coauthors;
    }
    
    public int getCount() {
        return count;
    }
    
    static public int getMaxPublCount() {
        return maxPublCount;
    }
    
    static public int getMaxNameLength() {
        return maxNameLength;
    }
    
    static public Iterator iterator() {
        return personMap.values().iterator();
    }
    
    static public Person searchPerson(String name) {
        return (Person) personMap.get(name);
    }
    
    static public int numberOfPersons() {
        return personMap.size();
    }
    
    static public void enterPublications() {
        Iterator publIt = Publication.iterator();
        Publication publ;
        Person[] persArray;
        Person pers;
        
        while (publIt.hasNext()) {
            publ = (Publication) publIt.next();
            persArray = publ.getAuthors();
            if (persArray == null)
                continue;
            if (persArray.length == 0)
                continue;
            for (int i=0; i<persArray.length; i++) {
                pers = persArray[i];
                if (pers == null)
                    continue;
                if (pers.publs == null) {
                    pers.publs = new Publication[pers.count];
                    pers.tmp = 0;
                }
                pers.publs[pers.tmp++] = publ;
            }
        }
        
        Iterator persIt = Person.iterator();
        
        while (persIt.hasNext()) {
            pers = (Person) persIt.next();
            Person authors[];
            
            tmpSet.clear();
            if (pers.publs != null) {
                for (int i=0; i<pers.publs.length; i++) {
                    publ = pers.publs[i];
                    authors = publ.getAuthors();
                    if (authors == null)
                        continue;
                    for (int j=0; j<authors.length; j++) {
                        if (authors[j] == null)
                            continue;
                        if (authors[j] == pers)
                            continue;
                        tmpSet.add(authors[j]);
                    }
                }
            }
            pers.coauthors = (Person[]) tmpSet.toArray(new Person[tmpSet.size()]);
        }
    }
    
    static public void printCoauthorTable() {
        Iterator it = Person.iterator();
        int coauthors[] = new int[501];
        Person pers;
        int c;
        
        while (it.hasNext()) {
            pers = (Person) it.next();
            c = pers.getNumberOfCoauthors();
            if (c > 500)
                c = 500;
            coauthors[c]++;
        }
        System.out.println();
        System.out.println("Number of coauthors: Number of persons");
        int n = 0;
        for (int j=0; j <= 500; j++) {
            if (coauthors[j] == 0)
                continue;
            n++;
            System.out.print(j + ": " + coauthors[j]+ "  ");
            if (n%5 == 0)
                System.out.println();
        }
        System.out.println();      
    }
    
    static public void printNamePartTable() {
        int numberOfParts[] = new int[11];
        Iterator it = Person.iterator();
        Person pers;
        int x;
        
        while (it.hasNext()) {
            pers = (Person) it.next();
            x = pers.getNameParts().size();
            if (x>10)
                x = 10;
            numberOfParts[x]++;
        }
        System.out.println();
        System.out.println("Number of Name Parts: Number of Persons");
        for (int n = 0; n < 10; n++)
            System.out.print(n + ":" + numberOfParts[n] + " ");
        System.out.println(">=10:" + numberOfParts[10]);
    }
    
    static public void findSimilarNames() {
        Iterator it = Person.iterator();
        Person pers;
        
        System.out.println("Name part permutation:");
        while (it.hasNext()) {
            pers = (Person) it.next();
            for (int i=0; i<pers.coauthors.length; i++) {
                for (int j=i+1; j<pers.coauthors.length; j++) {
                    if (pers.coauthors[i].nameParts.containsAll(pers.coauthors[j].nameParts) ||
                        pers.coauthors[j].nameParts.containsAll(pers.coauthors[i].nameParts))
                        System.out.println(pers.name + ": " +
                                           pers.coauthors[i].name + " - " +
                                	       pers.coauthors[j].name);
                }
            }
        }
    }
}
