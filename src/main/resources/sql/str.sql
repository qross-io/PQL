DEBUG ON;

FOR $path IN (FILE LIST 'c:/io.Qross/Folder/oneapi') LOOP
    SET $content := FILE READ $path;
    PRINT $content;
    FOR $m IN /\(\/doc\/(\/[a-z]+)+\)/ MATCHES $content LOOP

    END LOOP;
    EXIT;
END LOOP;