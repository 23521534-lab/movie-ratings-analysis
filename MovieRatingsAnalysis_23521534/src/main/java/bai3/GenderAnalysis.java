import java.io.*;
import java.util.*;

public class GenderAnalysis {
    public static void main(String[] args) throws Exception {
        Map<String, Map<String, List<Double>>> movieGenderRatings = new HashMap<>();
        Map<String, String> movies = new HashMap<>();
        Map<String, String> users = new HashMap<>();

        // Đọc danh sách phim
        readMovies("movies.txt", movies);

        // Đọc thông tin user
        readUsers("users.txt", users);

        // Đọc ratings từ 2 file
        readRatings("ratings_1.txt", users, movies, movieGenderRatings);
        readRatings("ratings_2.txt", users, movies, movieGenderRatings);

        System.out.println("\n=== KET QUA BAI 3: SO SANH DANH GIA GIUA NAM VA NU ===");
        System.out.println("------------------------------------------------");

        List<String> sortedMovies = new ArrayList<>(movieGenderRatings.keySet());
        Collections.sort(sortedMovies);

        for (String movieTitle : sortedMovies) {
            Map<String, List<Double>> genderMap = movieGenderRatings.get(movieTitle);

            List<Double> maleRatings = genderMap.getOrDefault("M", new ArrayList<>());
            List<Double> femaleRatings = genderMap.getOrDefault("F", new ArrayList<>());

            double maleAvg = calculateAvg(maleRatings);
            double femaleAvg = calculateAvg(femaleRatings);

            System.out.printf("%s:%n", movieTitle);
            System.out.printf("  Nam: %.2f (%d ratings)%n", maleAvg, maleRatings.size());
            System.out.printf("  Nu:  %.2f (%d ratings)%n%n", femaleAvg, femaleRatings.size());
        }
    }

    private static double calculateAvg(List<Double> ratings) {
        if (ratings.isEmpty()) return 0.0;
        double sum = 0;
        for (double r : ratings) sum += r;
        return sum / ratings.size();
    }

    private static void readMovies(String filename, Map<String, String> movies) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",", 3);
            if (fields.length >= 2) {
                String movieId = fields[0].trim();
                String title = fields[1].trim();
                movies.put(movieId, title);
            }
        }
        br.close();
    }

    private static void readUsers(String filename, Map<String, String> users) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(", ");
            if (fields.length >= 2) {
                String userId = fields[0].trim();
                String gender = fields[1].trim();
                users.put(userId, gender);
            }
        }
        br.close();
    }

    private static void readRatings(String filename, Map<String, String> users, 
                                    Map<String, String> movies,
                                    Map<String, Map<String, List<Double>>> movieGenderRatings) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(", ");
            if (fields.length >= 3) {
                String userId = fields[0].trim();
                String movieId = fields[1].trim();
                double rating = Double.parseDouble(fields[2].trim());

                String gender = users.get(userId);
                String movieTitle = movies.get(movieId);

                if (gender != null && movieTitle != null) {
                    movieGenderRatings.putIfAbsent(movieTitle, new HashMap<>());
                    Map<String, List<Double>> genderMap = movieGenderRatings.get(movieTitle);

                    genderMap.putIfAbsent(gender, new ArrayList<>());
                    genderMap.get(gender).add(rating);
                }
            }
        }
        br.close();
    }
}
