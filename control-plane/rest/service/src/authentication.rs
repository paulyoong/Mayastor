use actix_web::{dev::ServiceRequest, Error};
use actix_web_httpauth::extractors::bearer::BearerAuth;
use jsonwebtoken::{crypto::verify, Algorithm, DecodingKey};
use std::str::FromStr;

lazy_static! {
    static ref JWK: JsonWebKey = JsonWebKey {
        jwk: load_jwk(),
    };
}

// Currently load JWK from a test file.
fn load_jwk() -> serde_json::Value {
    let jwk = std::fs::read_to_string("control-plane/rest/jwt/jwk")
        .expect("Failed to read jwk");
    serde_json::from_str(&jwk).unwrap()
}

struct JsonWebKey {
    jwk: serde_json::Value,
}

impl JsonWebKey {
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

// Validate that the signature of the token is valid.
pub async fn authenticate(
    req: ServiceRequest,
    credentials: BearerAuth,
) -> Result<ServiceRequest, Error> {
    let token = credentials.token();
    let (message, signature) = split_token(&token);
    return match verify(
        &signature,
        &message,
        &JWK.decoding_key(),
        JWK.algorithm(),
    ) {
        Ok(true) => Ok(req),
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
//      \______  ________/ \___  ___/
//             \/              \/
//           message        signature
fn split_token(token: &str) -> (String, String) {
    let elems = token.split('.').collect::<Vec<&str>>();
    let message = format!("{}.{}", elems[0], elems[1]);
    let signature = elems[2];
    (message, signature.into())
}
