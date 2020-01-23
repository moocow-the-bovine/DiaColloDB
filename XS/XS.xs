/*-*- Mode: C -*-*/

/*==============================================================================
 * Header stuff
 */
extern "C" {
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include <inttypes.h>
/*#include "ppport.h"*/
}

#include "utils.h"
#include "cofgen.h"

/*==============================================================================
 * XS Guts
 */

MODULE = DiaColloDB::XS    PACKAGE = DiaColloDB::XS

##=====================================================================
## bootstrap
##=====================================================================
BOOT:
 {
 } 

##=====================================================================
## DiaColloDB::XS::Cofreqs
INCLUDE: XS/Cofreqs.xs
