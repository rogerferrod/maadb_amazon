import java.io.*;
import java.util.Map;

import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.WordToSentenceProcessor;
import edu.stanford.nlp.util.logging.RedwoodConfiguration;

import java.util.*;
import java.util.stream.Collectors;


public class Vader {

    private List<String> sentences;
    private String sentence;
    private Map<String, Float> lexicon;
    private Map<String, Float> emoticon;
    private Map<String, String> slang;
    private Map<String, Float> modifier;
    private ArrayList<String> negate;
    private List<String> tokens;
    private final float CAP_SCALAR = 0.733f;
    private final float NEG_SCALAR = -0.74f;
    static public final float B_INCR = 0.293f;

    private final String[] punctuation = {",", ":", ";", "-", "_", "=", "^", "$", "%",
            "&", "/", "(", ")", "|", "£", "+", "<", ">", "#", "§", "[", "]", "{", "}",
            "°", "*", "."};

    public Vader(Map<String, Float> lexicon, Map<String, Float> emoticon, Map<String, String> slang, Map<String, Float> modifier, ArrayList<String> negate, List<String> sentences) throws IOException {
        this.lexicon = lexicon;
        this.emoticon = emoticon;
        this.slang = slang;
        this.modifier = modifier;
        this.negate = negate;
        this.sentences = sentences;
        this.tokens = new ArrayList<>();
        this.sentence = null;
    }

    public Vader(Map<String, Float> lexicon, Map<String, Float> emoticon, Map<String, String> slang, Map<String, Float> modifier, ArrayList<String> negate, String sentence) throws IOException {
        this.lexicon = lexicon;
        this.emoticon = emoticon;
        this.slang = slang;
        this.modifier = modifier;
        this.negate = negate;
        this.sentences = null;
        this.sentence = sentence;
        this.tokens = new ArrayList<>();
    }

    private void substituteEmoticon() {
        String[] splits = sentence.split(" ");
        List<String> emoticonIndexes = new ArrayList<>(emoticon.keySet());
        for (int i = 0; i < splits.length; i++) {
            if (emoticon.containsKey(splits[i])) {
                splits[i] = "1596EMOTICON" + emoticonIndexes.indexOf(splits[i]);
            }
        }
        sentence = Arrays.stream(splits).collect(Collectors.joining(" "));
    }

    private void substituteSlang() {
        for (int i = 0; i < tokens.size(); i++) {
            String value = slang.get(tokens.get(i));
            if (value != null) {
                String[] slang = value.split(" ");
                tokens.remove(i);
                ArrayList<String> toAdd = new ArrayList<>(Arrays.asList(slang));
                tokens.addAll(i, toAdd);
            }
        }
    }

    private void tokenize() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        CoreDocument exampleDocument = new CoreDocument(sentence);
        pipeline.annotate(exampleDocument);
        List<CoreLabel> firstSentenceTokens = exampleDocument.sentences().get(0).tokens();
        for (CoreLabel token : firstSentenceTokens) {
            tokens.add(token.word());
        }
    }


    private void removePuntuation() {
        List<String> punt = Arrays.asList(punctuation);
        for (int i = 0; i < tokens.size(); i++) {
            if (punt.contains(tokens.get(i))) {
                tokens.remove(i);
            }
        }
    }

    private float evaluateVader() {
        //valuto se frase è tutta minuscola o maiuscola
        //capDifference = se true ci sono parole sia maiuscole che minuscole
        int countUpperWord = 0;
        for (String token : tokens) {
            if (isUpperWord(token)) {
                countUpperWord++;
            }
        }

        boolean isCapDifference = (countUpperWord != 0 && countUpperWord != tokens.size());


        List<Float> tokenScores = new ArrayList<>();
        List<String> tokenLowers = tokens.stream().map(String::toLowerCase).collect(Collectors.toList());

        for (int i = 0; i < tokens.size(); i++) {
            float score = 0.0f;
            if (!modifier.containsKey(tokens.get(i))) { //se tokens non è un modificatore
                /* LEXICON */
                String tokenLow = tokens.get(i).toLowerCase();
                Float lexValue = lexicon.get(tokenLow);
                if (lexValue != null) {
                    score = lexValue;

                    /* CAPITAL LETTER HEURISTIC */
                    if (isUpperWord(tokens.get(i)) && isCapDifference) {
                        if (score > 0) {
                            score = score + CAP_SCALAR;
                        } else {
                            score = score - CAP_SCALAR;
                        }
                    }

                    /* TRIGRAM HEURISTIC */
                    //Trigram Evaluation modica la valenza della parola andando a considerare fino ad un massimo di 3 parole precedenti
                    for (int j = 0; j < 3; j++) {
                        if (i > j && !lexicon.containsKey(tokens.get(i - (j + 1)).toLowerCase())) {
                            float scalar = scalarIncDec(tokens.get(i - (j + 1)), score, isCapDifference);
                            if (j == 1 && scalar != 0) {
                                scalar *= 0.95f;
                            }
                            if (j == 2 && scalar != 0) {
                                scalar *= 0.90f;
                            }
                            score += scalar;
                            score = negationCheck(tokenLowers, score, j, i);
                        }
                    }
                }
            }
            tokenScores.add(score);
        } //forEach token

        tokenScores = butCheck(tokenLowers, tokenScores);
        return scoreValence(tokenScores, sentence);
    }

    private float scoreValence(List<Float> tokenScores, String sentenceText) {
        float compound = 0.0f;
        if (!tokenScores.isEmpty()) {
            float sum = 0;
            for (float t : tokenScores) {
                sum = sum + t;
            }
            float amplifier = punctuationEmphasis(sentenceText);
            if (sum > 0) {
                sum += amplifier;
            } else if (sum < 0) {
                sum -= amplifier;
            }

            compound = normalize(sum);
        }

        return compound;
    }

    private float normalize(float score) {
        final float alpha = 15;
        float norm = (float) (score / (Math.sqrt((score * score) + alpha)));
        if (norm < -1.0) {
            return -1.0f;
        } else if (norm > 1.0) {
            return 1.0f;
        }
        return norm;
    }

    private float punctuationEmphasis(String text) {
        float ep_amplifier = amplify_ep(text);
        float qm_amplifier = amplify_qm(text);
        return ep_amplifier + qm_amplifier;
    }

    private float amplify_qm(String text) {
        char[] temp = text.toCharArray();
        int count = 0;
        for (char c : temp) {
            if (c == '?') {
                count++;
            }
        }
        float qm_amplifier = 0;
        if (count > 1) {
            if (count <= 3) {
                qm_amplifier = count * 0.18f;
            } else {
                qm_amplifier = 0.96f;
            }
        }
        return qm_amplifier;
    }

    private float amplify_ep(String text) {
        char[] temp = text.toCharArray();
        int count = 0;
        for (char c : temp) {
            if (c == '!') {
                count++;
            }
        }
        if (count > 4) {
            count = 4;
        }

        return count * 0.292f;
    }

    private List<Float> butCheck(List<String> tokenLowers, List<Float> tokenScores) {
        if (tokenLowers.contains("but")) {
            int bi = tokenLowers.indexOf("but");
            for (float sentiment : tokenScores) {
                int si = tokenScores.indexOf(sentiment);
                if (si < bi) {
                    tokenScores.set(si, sentiment * 0.5f);
                } else if (si > bi) {
                    tokenScores.set(si, sentiment * 1.5f);
                }
            }
        }
        return tokenScores;
    }

    private float negationCheck(List<String> tokenLowers, float score, int j, int i) {
        if (j == 0) {
            if (negated(tokenLowers.get(i - (j + 1)))) {
                score *= NEG_SCALAR;
            }
        }
        if (j == 1) {
            if (tokenLowers.get(i - 2).equals("never") && (tokenLowers.get(i - 1).equals("so") || tokenLowers.get(i - 1).equals("this"))) {
                // 1 word preceding lexicon word (w/o stopwords)
                score *= 1.25f;
            } else if (tokenLowers.get(i - 2).equals("without") && tokenLowers.get(i - 1).equals("doubt")) {
                score = score; //ummmm
            } else if (negated(tokenLowers.get(i - (j + 1)))) {
                // 2 words preceding the lexicon word position
                score = score * NEG_SCALAR;
            }
        }
        if (j == 2) {
            if (tokenLowers.get(i - 3).equals("never") && (tokenLowers.get(i - 2).equals("so") || tokenLowers.get(i - 2).equals("this")) || (tokenLowers.get(i - 1).equals("so") || tokenLowers.get(i - 1).equals("this"))) {
                score *= 1.25f;
            } else if (tokenLowers.get(i - 3).equals("without") && (tokenLowers.get(i - 2).equals("doubt") || tokenLowers.get(i - 1).equals("doubt"))) {
                score = score; //ummm
            } else if (negated(tokenLowers.get(i - (j + 1)))) {
                // 3 words preceding the lexicon word position
                score *= NEG_SCALAR;
            }
        }
        return score;
    }

    private boolean negated(String tokenLow) {
        return negate.contains(tokenLow) || tokenLow.equals("n't");
    }


    private float scalarIncDec(String word, float score, boolean cap_difference) {
        //Check if the preceding words increase, decrease, or negate/nullify the valence
        String word_low = word.toLowerCase();
        float scalar = 0.0f;
        if (modifier.containsKey(word_low)) {
            scalar = modifier.get(word_low);
            if (score < 0) {
                scalar *= -1;
            }
            if (isUpperWord(word) && cap_difference) {//se parola è tutta maiuscola e ho parole maiuscole e minuscole nel testo
                if (score > 0) //se è parola positiva aumento positività
                    scalar += CAP_SCALAR;
                else //altrimenti aumento negatività
                    scalar -= CAP_SCALAR;
            }
        }
        return scalar;
    }

    private boolean isUpperWord(String word) {//se ritorna false --> parola non è tutta maiuscola
        boolean res = true;
        for (char c : word.toCharArray()) {
            if (Character.isLetter(c) && !Character.isUpperCase(c)) {
                res = false;
                break;
            }
        }
        return res;
    }

    public static List<String> tokenizeSentence(String text) {
        List<CoreLabel> tokens = new ArrayList<>();
        PTBTokenizer<CoreLabel> tokenizer = new PTBTokenizer<>(new StringReader(text), new CoreLabelTokenFactory(), "");
        while (tokenizer.hasNext()) {
            tokens.add(tokenizer.next());
        }

        List<List<CoreLabel>> sentences = new WordToSentenceProcessor<CoreLabel>().process(tokens);
        int end;
        int start = 0;
        List<String> sentenceList = new ArrayList<>();
        for (List<CoreLabel> sentence : sentences) {
            end = sentence.get(sentence.size() - 1).endPosition();
            sentenceList.add(text.substring(start, end).trim());
            start = end;
        }
        return sentenceList;
    }

    private float computeScore(String sent) {
        sentence = sent;
        substituteEmoticon();
        tokenize();
        substituteSlang();
        removePuntuation();
        return evaluateVader();
    }

    public float evaluate() {
        //elaboro le frasi/frase

        RedwoodConfiguration.current().clear().apply(); //disable logging

        if (this.sentences == null) {
            return computeScore(sentence);
        }

        float tot = 0.0f;
        float polarity;
        for (String sent : sentences) {
            polarity = computeScore(sent);
            //System.out.println("FRASE : " + sent + " PUNTEGGIO : " + polarity);
            tot += polarity;

            tokens.clear();
            sentence = null;
        }
        return tot / sentences.size();
    }

    public static Map<String, Float> makeLexiconDictionary(String path) throws IOException {
        try {
            FileReader input = new FileReader(path);
            BufferedReader bufRead = new BufferedReader(input);
            String myLine;
            Map<String, Float> dictionary = new HashMap<>();
            while ((myLine = bufRead.readLine()) != null) {
                String[] splits = myLine.split("\t");
                dictionary.put(splits[0], Float.parseFloat(splits[1]));
            }
            return dictionary;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Map<String, Float> makeEmoticonDictionary(String path) throws IOException {
        try {
            FileReader input = new FileReader(path);
            BufferedReader bufRead = new BufferedReader(input);
            String myLine;
            Map<String, Float> dictionary = new HashMap<>();
            while ((myLine = bufRead.readLine()) != null) {
                String[] splits = myLine.split("\t");
                dictionary.put(splits[0], Float.parseFloat(splits[1]));
            }
            return dictionary;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Map<String, Float> makeModifierDictionary(String path) throws IOException {
        try {
            FileReader input = new FileReader(path);
            BufferedReader bufRead = new BufferedReader(input);
            String myLine;
            Map<String, Float> dictionary = new HashMap<>();
            while ((myLine = bufRead.readLine()) != null) {
                String[] splits = myLine.split("\t");
                dictionary.put(splits[0], (splits[1].equals("+")) ? Vader.B_INCR : -Vader.B_INCR);
            }
            return dictionary;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Map<String, String> makeSlangDictionary(String path) throws IOException {
        try {
            FileReader input = new FileReader(path);
            BufferedReader bufRead = new BufferedReader(input);
            String myLine;
            Map<String, String> dictionary = new HashMap<>();
            while ((myLine = bufRead.readLine()) != null) {
                String[] splits = myLine.split(";");
                dictionary.put(splits[0], splits[1]);
            }
            return dictionary;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static ArrayList<String> makeNegateList(String path) throws IOException {
        try {
            FileReader input = new FileReader(path);
            BufferedReader bufRead = new BufferedReader(input);
            String myLine;
            ArrayList<String> dictionary = new ArrayList<>();
            while ((myLine = bufRead.readLine()) != null) {
                dictionary.add(myLine);
            }
            return dictionary;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        try {
            List<String> sentences = new ArrayList<>();
            sentences.add("VADER is smart, handsome, and funny.");
            sentences.add("VADER is smart, handsome, and funny!.");
            sentences.add("VADER is very smart, handsome, and funny.");
            sentences.add("VADER is VERY SMART, handsome, and FUNNY.");
            sentences.add("VADER is VERY SMART, handsome, and FUNNY!!!");
            sentences.add("VADER is VERY SMART, uber handsome, and FRIGGIN FUNNY!!!");
            sentences.add("VADER is not smart, handsome, nor funny.");
            sentences.add("The book was good.");
            sentences.add("At least it isn't a horrible book.");
            sentences.add("The book was only kind of good.");
            sentences.add("The plot was good, but the characters are uncompelling and the dialog is not great.");
            sentences.add("Today SUX!");
            sentences.add("Today only kinda sux! But I'll get by, lol.");
            sentences.add("Make sure you :) or :D today!");
            sentences.add("Not bad at all.");
            sentences.add("SQLSentiment analysis has never been good.");
            sentences.add("SQLSentiment analysis has never been this good!");
            sentences.add("Most automated sentiment analysis tools are shit.");
            sentences.add("With VADER, sentiment analysis is the shit!");
            sentences.add("Other sentiment analysis tools can be quite bad.");
            sentences.add("On the other hand, VADER is quite bad ass.");
            sentences.add("VADER is such a badass!");
            sentences.add("Without a doubt, excellent idea.");
            sentences.add("Roger Dodger is one of the most compelling variations on this theme.");
            sentences.add("Roger Dodger is at least compelling as a variation on the theme.");
            sentences.add("Roger Dodger is one of the least compelling variations on this theme.");
            sentences.add("Not such a badass after all.");
            sentences.add("Without a doubt, an excellent idea.");
            sentences.add("imho this is great");


            Map<String, Float> lexicon = Vader.makeLexiconDictionary(args[0]);
            Map<String, Float> emoticon = Vader.makeEmoticonDictionary(args[1]);
            Map<String, String> slang = Vader.makeSlangDictionary(args[2]);
            Map<String, Float> modifier = Vader.makeModifierDictionary(args[3]);
            ArrayList<String> negate = Vader.makeNegateList(args[4]);
            Vader vader = new Vader(lexicon, emoticon, slang, modifier, negate, sentences);
            float tot = vader.evaluate();
            System.out.println("PUNTEGGIO TOTALE : " + tot);


            List<String> sentList = Vader.tokenizeSentence("prova a tokenizzarmi questo! funziona? ciao mondo!!!");
            System.out.println(sentList);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}