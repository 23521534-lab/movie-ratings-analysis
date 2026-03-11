import java.io.*;
import java.util.*;

public class GenreAnalysis {
    public static void main(String[] args) throws Exception {
        Map<String, List<Double>> genreRatings = new HashMap<>();
        Map<String, String> movieGenres = new HashMap<>();

        // Đọc thể loại phim
        readMovies("movies.txt", movieGenres);

        // Đọc ratings từ 2 file
        readRatings("ratings_1.txt", movieGenres, genreRatings);
        readRatings("ratings_2.txt", movieGenres, genreRatings);

        System.out.println("\n=== KET QUA BAI 2: DIEM TRUNG BINH THEO THE LOAI ===");
        System.out.println("------------------------------------------------");

        List<String> sortedGenres = new ArrayList<>(genreRatings.keySet());
        Collections.sort(sortedGenres);

        for (String genre : sortedGenres) {
            List<Double> list = genreRatings.get(genre);
            int count = list.size();
            double sum = 0;
            for (double r : list) sum += r;
            double avg = sum / count;

            System.out.printf("%-15s: %.2f (Total: %d ratings)%n", genre, avg, count);
        }
    }

    private static void readMovies(String filename, Map<String, String> movieGenres) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",", 3);
            if (fields.length >= 3) {
                String movieId = fields[0].trim();
                String genres = fields[2].trim();
                movieGenres.put(movieId, genres);
            }
        }
        br.close();
    }

    private static void readRatings(String filename, Map<String, String> movieGenres, 
                                    Map<String, List<Double>> genreRatings) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(", ");
            if (fields.length >= 3) {
                String movieId = fields[1].trim();
                double rating = Double.parseDouble(fields[2].trim());

                String genres = movieGenres.get(movieId);
                if (genres != null) {
                    for (String genre : genres.split("\\|")) {
                        genre = genre.trim();
                        genreRatings.putIfAbsent(genre, new ArrayList<>());
                        genreRatings.get(genre).add(rating);
                    }
                }
            }
        }
        br.close();
    }
}
