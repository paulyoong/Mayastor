use actix_web::{Error, HttpRequest};
use jsonwebtoken::{crypto, Algorithm, DecodingKey};
use std::str::FromStr;

use actix_web::http::header::Header;
use actix_web_httpauth::headers::authorization;
use once_cell::sync::OnceCell;
use std::fs::File;
static JWK: OnceCell<JsonWebKey> = OnceCell::new();

/// Initialise JWK with the contents of the file at 'jwk_path'.
/// If jwk_path is 'None', authentication is disabled.
pub fn init(jwk_path: Option<String>) {
    let jwk = match jwk_path {
        Some(path) => {
            let jwk_file = File::open(path).expect("Failed to open JWK file");
            let jwk = serde_json::from_reader(jwk_file)
                .expect("Failed to deserialise JWK");
            JsonWebKey {
                jwk,
            }
        }
        None => JsonWebKey {
            ..Default::default()
        },
    };
    JWK.set(jwk).expect("Failed to set JWK");
}

fn jwk() -> &'static JsonWebKey {
    JWK.get().expect("Failed to get JWK")
}

#[derive(Default, Debug)]
struct JsonWebKey {
    jwk: serde_json::Value,
}

impl JsonWebKey {
    // Returns true if REST calls should be authenticated.
    fn auth_enabled(&self) -> bool {
        !self.jwk.is_null()
    }

    // Return the algorithm.
    fn algorithm(&self) -> Algorithm {
        Algorithm::from_str(self.jwk["alg"].as_str().unwrap()).unwrap()
    }

    // Return the modulus.
    fn modulus(&self) -> &str {
        self.jwk["n"].as_str().unwrap()
    }

    // Return the exponent.
    fn exponent(&self) -> &str {
        self.jwk["e"].as_str().unwrap()
    }

    // Return the decoding key
    fn decoding_key(&self) -> DecodingKey {
        DecodingKey::from_rsa_components(self.modulus(), self.exponent())
    }
}

/// Authenticate the HTTP request by checking the authorisation token to ensure
/// the sender is who they claim to be.
pub fn authenticate(req: &HttpRequest) -> Result<(), Error> {
    // If authentication is disabled there is nothing to do.
    if !jwk().auth_enabled() {
        return Ok(());
    }

    // Check the request for a bearer token.
    // An error is returned if the bearer token cannot be found.
    match authorization::Authorization::<authorization::Bearer>::parse(req) {
        Ok(bearer_auth) => validate(bearer_auth.into_scheme().token()),
        Err(_) => {
            tracing::error!("Missing bearer token in HTTP request.");
            Err(Error::from(actix_web::HttpResponse::Unauthorized()))
        }
    }
}

/// Validate a bearer token.
pub fn validate(token: &str) -> Result<(), Error> {
    let (message, signature) = split_token(&token);
    return match crypto::verify(
        &signature,
        &message,
        &jwk().decoding_key(),
        jwk().algorithm(),
    ) {
        Ok(true) => Ok(()),
        Ok(false) => {
            tracing::error!("Signature verification failed.");
            Err(Error::from(actix_web::HttpResponse::Unauthorized()))
        }
        Err(e) => {
            tracing::error!(
                "Failed to complete signature verification with error {}",
                e
            );
            Err(Error::from(actix_web::HttpResponse::Unauthorized()))
        }
    };
}

// Split the JSON Web Token (JWT) into 2 parts, message and signature.
// The message comprises the header and payload.
//
// JWT format:
//      <header>.<payload>.<signature>
//      \______  ________/
//             \/
//           message
fn split_token(token: &str) -> (String, String) {
    let elems = token.split('.').collect::<Vec<&str>>();
    let message = format!("{}.{}", elems[0], elems[1]);
    let signature = elems[2];
    (message, signature.into())
}
