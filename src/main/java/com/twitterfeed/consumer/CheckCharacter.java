package com.twitterfeed.consumer;

import org.apache.avro.generic.GenericData;

import java.util.Arrays;
import java.util.List;

/**
 * Created by franco on 11/05/17.
 */
public  class CheckCharacter {

    public static boolean isCharacter(String characterCompare)
    {
        characterCompare.toLowerCase();
        String characters = "<>&\\!#$%(*+,-./:;=@·~½_^'_|`]}{[RT";
        List<String> wordsOmmitted = Arrays.asList("the","a","de","por","para","el", "la", "con", "with" );

        if(characters.indexOf(characterCompare)>0)
        {
            return true;
        }

        for (String w : wordsOmmitted)
        {
         if(w.compareTo(characterCompare)== 0)
         {
             return true;
         }
        }

        return false;



    }


}
