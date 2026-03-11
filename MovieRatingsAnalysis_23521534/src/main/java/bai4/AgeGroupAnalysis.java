import java.io.*;
import java.util.*;

public class AgeGroupAnalysis {
    private static final String[] AGE_GROUPS = {"0-18", "19-35", "36-50", "50+"};

    public static void main(String[] args) throws Exception {
        Map<String, Map<String, List<Double>>> movieAgeRatings = new HashMap<>();
        Map<String, String> movies = new HashMap<>();
        Map<String, Integer> users = new HashMap<>();

        // Đọc danh sách phim
        readMovies("movies.txt", movies);

        // Đọc thông tin user
        readUsers("users.txt", users);

        // Đọc ratings từ 2 file
        readRatings("ratings_1.txt", users, movies, movieAgeRatings);
        readRatings("ratings_2.txt", users, movies, movieAgeRatings);

        System.out.println("\n=== KET QUA BAI 4: PHAN TICH DANH GIA THEO NHOM TUOI ===");
        System.out.println("------------------------------------------------");

        List<String> sortedMovies = new ArrayList<>(movieAgeRatings.keySet());
        Collections.sort(sortedMovies);

        for (String movieTitle : sortedMovies) {
            System.out.println(movieTitle + ":");
            Map<String, List<Double>> ageMap = movieAgeRatings.get(movieTitle);

            for (String ageGroup : AGE_GROUPS) {
                List<Double> ratings = ageMap.getOrDefault(ageGroup, new ArrayList<>());
                double avg = calculateAvg(ratings);
                System.out.printf("  %-6s: %.2f (%d ratings)%n", ageGroup, avg, ratings.size());
            }
            System.out.println();
        }
    }

    private static String getAgeGroup(int age) {
        if (age <= 18) return "0-18";
        if (age <= 35) return "19-35";
        if (age <= 50) return "36-50";
        return "50+";
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

    private static void readUsers(String filename, Map<String, Integer> users) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(", ");
            if (fields.length >= 3) {
                String userId = fields[0].trim();
                int age = Integer.parseInt(fields[2].trim());
                users.put(userId, age);
            }
        }
        br.close();
    }

    private static void readRatings(String filename, Map<String, Integer> users,
                                    Map<String, String> movies,
                                    Map<String, Map<String, List<Double>>> movieAgeRatings) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(", ");
            if (fields.length >= 3) {
                String userId = fields[0].trim();
                String movieId = fields[1].trim();
                double rating = Double.parseDouble(fields[2].trim());

                Integer age = users.get(userId);
                String movieTitle = movies.get(movieId);

                if (age != null && movieTitle != null) {
                    String ageGroup = getAgeGroup(age);

                    movieAgeRatings.putIfAbsent(movieTitle, new HashMap<>());
                    Map<String, List<Double>> ageMap = movieAgeRatings.get(movieTitle);

                    ageMap.putIfAbsent(ageGroup, new ArrayList<>());
                    ageMap.get(ageGroup).add(rating);
                }
            }
        }
        br.close();
    }
}
