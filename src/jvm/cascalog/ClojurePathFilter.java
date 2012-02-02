package cascalog;

import clojure.lang.IFn;
import clojure.lang.RT;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class ClojurePathFilter {
    Object[] spec;
    transient IFn fn = null;

    public ClojurePathFilter(Object[] spec) {
        this.spec = spec;
    }

    public boolean accept(Path path) throws IOException {
        if(this.fn == null)
            this.fn = Util.bootFn(this.spec);

        try {
            return RT.booleanCast(this.fn.invoke(path));
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}