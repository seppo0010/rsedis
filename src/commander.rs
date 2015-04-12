use super::database::Database;
use super::parser::Parser;

pub enum Response {
    Nil,
    Data(Vec<u8>),
    Err(String),
    Status(String),
}

pub fn set(parser: &Parser, db: &mut Database) -> Response {
    if parser.argc != 3 {
        return Response::Err("Wrong number of parameters".to_string());
    }
    let try_key = parser.get_vec(1);
    if try_key.is_err() {
        return Response::Err("Invalid key".to_string());
    }
    let try_val = parser.get_vec(2);
    if try_val.is_err() {
        return Response::Err("Invalid value".to_string());
    }
    db.set(&try_key.unwrap(), try_val.unwrap());
    return Response::Status("OK".to_string());
}

pub fn run(parser: &Parser, db: &mut Database) -> Response {
    if parser.argc == 0 {
        return Response::Err("Not enough arguments".to_string());
    }
    let try_command = parser.get_str(0);
    if try_command.is_err() {
        return Response::Err("Invalid command".to_string());
    }
    match try_command.unwrap() {
        "set" => return set(parser, db),
        _ => return Response::Err("Uknown command".to_string()),
    };
}
