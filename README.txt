    README for DiaColloDB

ABSTRACT
    DiaColloDB - diachronic collocation database

REQUIREMENTS
    DB_File
        For handling large temporary hashes during index construction.
        Available from CPAN.

    DDC::Concordance (formerly ddc-perl)
        Perl module for DDC client connections. Available from CPAN, or via
        SVN from
        <https://svn.code.sf.net/p/ddc-concordance/code/ddc-perl/trunk>

    DDC::XS (formerly ddc-perl-xs)
        XS wrappers for DDC query parsing. Available from CPAN, or via SVN
        from
        <https://svn.code.sf.net/p/ddc-concordance/code/ddc-perl-xs/trunk>

    File::Map
        Available from CPAN.

    File::Path
        Available from CPAN.

    File::Temp
        Available from CPAN.

    JSON
        Available from CPAN.

    IPC::Run
        Available from CPAN.

    Log::Log4perl
        Available from CPAN.

    PDL (optional)

        Perl Data Language for fast fixed-size numeric data structures, used
        by the TDF (term-document frequency matrix) relation type, available
        from CPAN.

        It should still be possible to build, install, and run the
        DiaColloDB distribution on a system without PDL installed, but use
        of the the TDF (term x document) matrix relation type will be
        disabled.

    PDL::CCS
        (optional)

        PDL module for sparse index-encoded matrices, used by the TDF
        (term-document frequency matrix) relation type, available from CPAN.
        See the caveats under PDL.

    Tie::File::Indexed
        For handling large (temporary) arrays during index creation,
        available from CPAN.

    (a corpus to index or an existing index to query)
        Currently, only the ddc_dump "tabs" format is supported for corpus
        indexing. Additional formats can be supported by implementing a
        subclass of DiaColloDB::Document forp parsing input documents.

DESCRIPTION
    The DiaColloDB package provides a set of object-oriented Perl modules
    and a command-line utility suite for constructing and querying native
    diachronic collocation indices with optional inclusion of a DDC server
    back-end for fine-grained queries.

INSTALLATION
    Issue the following commands to the shell:

     bash$ cd DiaColloDB-0.01 # (or wherever you unpacked this distribution)
     bash$ perl Makefile.PL   # check requirements, etc.
     bash$ make               # build the module
     bash$ make test          # (optional): test module before installing
     bash$ make install       # install the module on your system

SEE ALSO
    *   <http://kaskade.dwds.de/dstar/dta/diacollo/> contains a live web
        demo of a DiaCollo index on the *Deutsches Textarchiv* corpus of
        historical German, including a user-oriented help page (in English).

        See <http://kaskade.dwds.de/dstar/dta/diacollo/>.

AUTHOR
    Bryan Jurish <moocow@cpan.org>

