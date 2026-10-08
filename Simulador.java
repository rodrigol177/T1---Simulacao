import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Simulador de rede de filas G/G/c/K orientado a eventos (formato .yml do módulo 3).
 * Uso:  java Simulador.java modelo.yml [semente] [qtdAleatorios]
 *  (ou: javac Simulador.java && java Simulador modelo.yml)
 *  - com 'seeds' + 'rndnumbersPerSeed': uma simulação por semente
 *  - sem 'seeds': usa a lista 'rndnumbers' do arquivo
 * Sem dependências externas (leitor de .yml próprio para o formato do módulo 3).
 */
public class Simulador {

    /** Lançada ao tentar usar um aleatório além do limite. */
    static class StopSimulation extends RuntimeException {}

    /** Fonte de aleatórios: LCG do módulo 3 (X(i+1) = (a*X(i)+c) mod m; u = X/m) ou lista fixa. */
    static class Rng {
        final long limit; long count = 0;
        long x; final long a, c, m; final double[] list;
        Rng(long seed, long limit, long a, long c, long m) {
            this.x = seed; this.limit = limit; this.a = a; this.c = c; this.m = m; this.list = null;
        }
        Rng(double[] list) { this.list = list; this.limit = list.length; this.a = this.c = this.m = 0; }
        double next() {
            if (count >= limit) throw new StopSimulation();
            double u;
            if (list != null) u = list[(int) count];
            else { x = (a * x + c) % m; u = (double) x / (double) m; }
            count++;
            return u;
        }
    }

    // ---------- definição do modelo (lida do .yml) ----------
    static class QueueDef {
        final String name; int servers = 1; Integer capacity;      // capacity == null -> infinita
        Double minA, maxA; double minS, maxS;
        QueueDef(String n) { name = n; }
    }
    static class RouteDef {
        String source, target; double probability;
    }

    static final LinkedHashMap<String, QueueDef> defs = new LinkedHashMap<>();
    static final LinkedHashMap<String, Double> firstArrivals = new LinkedHashMap<>();
    static final List<RouteDef> routeDefs = new ArrayList<>();
    static final List<Double> rndList = new ArrayList<>();
    static final List<Long> seeds = new ArrayList<>();
    static long perSeed = 100000, lcgA = 16807, lcgC = 2, lcgM = 2147483647L;

    /** Leitor mínimo do formato .yml do módulo 3 (linha '!PARAMETERS', listas '-' etc.). */
    static void load(Path path) throws IOException {
        String section = "";
        QueueDef curQ = null; RouteDef curR = null;
        for (String raw : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            int h = raw.indexOf('#');
            String line = h >= 0 ? raw.substring(0, h) : raw;           // remove comentários
            String t = line.trim();
            if (t.isEmpty() || t.startsWith("!")) continue;
            boolean indented = Character.isWhitespace(line.charAt(0));

            if (!indented && !t.startsWith("-")) {                       // chave de nível 0
                int i = t.indexOf(':');
                String key = t.substring(0, i).trim(), val = t.substring(i + 1).trim();
                section = key; curQ = null; curR = null;
                if (!val.isEmpty()) {
                    if (key.equals("rndnumbersPerSeed")) perSeed = Long.parseLong(val);
                    else if (key.equals("lcgA")) lcgA = Long.parseLong(val);
                    else if (key.equals("lcgC")) lcgC = Long.parseLong(val);
                    else if (key.equals("lcgM")) lcgM = Long.parseLong(val);
                }
                continue;
            }
            if (t.startsWith("-")) {                                     // item de lista
                String item = t.substring(1).trim();
                if (section.equals("rndnumbers")) rndList.add(Double.parseDouble(item));
                else if (section.equals("seeds")) seeds.add(Long.parseLong(item));
                else if (section.equals("network")) {
                    curR = new RouteDef(); routeDefs.add(curR);
                    if (!item.isEmpty()) setRoute(curR, item);
                }
                continue;
            }
            // linha indentada "chave: valor"
            int i = t.indexOf(':');
            String key = t.substring(0, i).trim(), val = t.substring(i + 1).trim();
            if (section.equals("arrivals")) firstArrivals.put(key, Double.parseDouble(val));
            else if (section.equals("queues")) {
                if (val.isEmpty()) { curQ = new QueueDef(key); defs.put(key, curQ); }
                else if (curQ != null) {
                    double d = Double.parseDouble(val);
                    if (key.equals("servers")) curQ.servers = (int) d;
                    else if (key.equals("capacity")) curQ.capacity = (int) d;
                    else if (key.equals("minArrival")) curQ.minA = d;
                    else if (key.equals("maxArrival")) curQ.maxA = d;
                    else if (key.equals("minService")) curQ.minS = d;
                    else if (key.equals("maxService")) curQ.maxS = d;
                }
            } else if (section.equals("network") && curR != null) setRoute(curR, t);
        }
        for (String n : firstArrivals.keySet())
            if (defs.get(n).minA == null || defs.get(n).maxA == null)
                throw new IllegalArgumentException(n + " tem chegada externa mas não tem minArrival/maxArrival");
    }

    static void setRoute(RouteDef r, String kv) {
        int i = kv.indexOf(':');
        String key = kv.substring(0, i).trim(), val = kv.substring(i + 1).trim();
        if (key.equals("source")) r.source = val;
        else if (key.equals("target")) r.target = val;
        else if (key.equals("probability")) r.probability = Double.parseDouble(val);
    }

    // ---------- estado de uma simulação ----------
    static class Route {
        final String target; final double cumulative;
        Route(String t, double c) { target = t; cumulative = c; }
    }
    static class Queue {
        final QueueDef def; int count = 0, loss = 0;
        final TreeMap<Integer, Double> times = new TreeMap<>();
        final List<Route> routes = new ArrayList<>();
        Queue(QueueDef d) { def = d; }
    }
    static class Event {
        final double time; final int kind; final long seq; final String queue;
        Event(double time, int kind, long seq, String queue) {
            this.time = time; this.kind = kind; this.seq = seq; this.queue = queue;
        }
    }

    static final int DEPARTURE = 0, ARRIVAL = 1;   // em empate, a saída vem antes

    static LinkedHashMap<String, Queue> queues;
    static double now;
    static long seq;
    static Rng rng;
    static PriorityQueue<Event> events;

    static void build() {
        queues = new LinkedHashMap<>();
        for (QueueDef d : defs.values()) queues.put(d.name, new Queue(d));
        Map<String, Double> acc = new HashMap<>();
        for (RouteDef r : routeDefs) {
            double a = acc.merge(r.source, r.probability, Double::sum);
            if (a > 1.0 + 1e-9) throw new IllegalArgumentException("Probabilidades de " + r.source + " somam > 1");
            queues.get(r.source).routes.add(new Route(r.target, a));
        }
    }

    static void schedule(double t, int kind, String q) { events.add(new Event(t, kind, ++seq, q)); }
    static double uniform(double a, double b) { return a + (b - a) * rng.next(); }
    static void startService(Queue q) { schedule(now + uniform(q.def.minS, q.def.maxS), DEPARTURE, q.def.name); }

    static void arrive(Queue q) {
        if (q.def.capacity != null && q.count >= q.def.capacity) { q.loss++; return; }   // fila cheia: perda
        q.count++;
        if (q.count <= q.def.servers) startService(q);                                    // servidor livre
    }

    static void simulate() {
        now = 0.0; seq = 0;
        events = new PriorityQueue<>(Comparator.comparingDouble((Event e) -> e.time)
                .thenComparingInt(e -> e.kind).thenComparingLong(e -> e.seq));
        for (Map.Entry<String, Double> f : firstArrivals.entrySet())     // 1ª chegada: não consome aleatório
            schedule(f.getValue(), ARRIVAL, f.getKey());
        try {
            while (!events.isEmpty()) {
                Event e = events.poll();
                for (Queue q : queues.values())                           // acumula tempo no estado vigente
                    q.times.merge(q.count, e.time - now, Double::sum);
                now = e.time;
                Queue q = queues.get(e.queue);
                if (e.kind == ARRIVAL) {
                    arrive(q);
                    schedule(now + uniform(q.def.minA, q.def.maxA), ARRIVAL, q.def.name);   // próxima chegada externa
                } else {
                    q.count--;
                    if (q.count >= q.def.servers) startService(q);
                    if (!q.routes.isEmpty()) {
                        double u = rng.next();                            // sorteio do destino (faixas acumuladas)
                        for (Route r : q.routes)
                            if (u < r.cumulative) { arrive(queues.get(r.target)); break; }
                        // senão: sai do sistema
                    }
                }
                if (rng.count >= rng.limit) break;                        // usou o último aleatório: encerra
            }
        } catch (StopSimulation ignored) { }
    }

    static String report(String title) {
        StringBuilder sb = new StringBuilder();
        sb.append("######## ").append(title).append(" ########\n");
        sb.append("Aleatórios utilizados: ").append(rng.count).append('\n');
        sb.append(String.format(Locale.US, "Tempo global da simulação: %.4f%n%n", now));
        for (Queue q : queues.values()) {
            sb.append("=== ").append(q.def.name).append("  G/G/").append(q.def.servers).append('/')
              .append(q.def.capacity == null ? "∞" : q.def.capacity.toString()).append(" ===\n");
            sb.append(String.format("%7s %18s %15s%n", "Estado", "Tempo acumulado", "Probabilidade"));
            for (Map.Entry<Integer, Double> en : q.times.entrySet())
                sb.append(String.format(Locale.US, "%7d %18.4f %14.4f%%%n",
                        en.getKey(), en.getValue(), en.getValue() / now * 100));
            sb.append("Clientes perdidos: ").append(q.loss).append("\n\n");
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) { System.err.println("Uso: java Simulador.java modelo.yml [semente] [qtdAleatorios]"); return; }
        load(Paths.get(args[0]));
        long n = args.length > 2 ? Long.parseLong(args[2]) : perSeed;
        PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8);

        List<String> titles = new ArrayList<>(); List<Rng> sources = new ArrayList<>();
        if (args.length > 1) {
            long s = Long.parseLong(args[1]);
            titles.add("Semente " + s + " (" + n + " aleatórios)"); sources.add(new Rng(s, n, lcgA, lcgC, lcgM));
        } else if (!seeds.isEmpty()) {
            for (long s : seeds) { titles.add("Semente " + s + " (" + n + " aleatórios)"); sources.add(new Rng(s, n, lcgA, lcgC, lcgM)); }
        } else if (!rndList.isEmpty()) {
            double[] v = rndList.stream().mapToDouble(Double::doubleValue).toArray();
            titles.add("Lista de " + v.length + " aleatórios do arquivo"); sources.add(new Rng(v));
        } else { System.err.println("Informe 'seeds' ou 'rndnumbers' no arquivo .yml"); return; }

        for (int i = 0; i < sources.size(); i++) {
            rng = sources.get(i);
            build();
            simulate();
            out.print(report(titles.get(i)));
        }
    }
}
