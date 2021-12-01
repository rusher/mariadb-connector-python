from enum import Enum


class LexState(Enum):
    NORMAL = 1
    STRING = 2
    SLASH_STAR_COMMENT = 3
    ESCAPE = 4
    EOL_COMMENT = 5
    BACKTICK = 6


class ClientParser:

    __slots__ = ('sql', 'query_parts', 'param_count')

    def __init__(self, sql: str, query_parts):
        self.sql = sql
        self.query_parts = query_parts
        self.param_count = len(query_parts) - 1


def parameter_parts(sql: str, no_backslash_escapes: bool) -> ClientParser:
    part_list = []
    state = LexState.NORMAL
    last_char = '\0'
    ending_semicolon = False
    single_quotes = False
    last_parameter_position = 0

    query_length = len(sql)
    for i in range(query_length):
        car = sql[i]
        if state == LexState.ESCAPE and not ((car == '\'' and single_quotes) or (car == '"' and not single_quotes)):
            state = LexState.STRING
            last_char = car
            continue

        if car == '*':
            if state == LexState.NORMAL and last_char == '/':
                state = LexState.SLASH_STAR_COMMENT
        elif car == '/':
            if state == LexState.SLASH_STAR_COMMENT and last_char == '*':
                state = LexState.NORMAL
            elif state == LexState.NORMAL and last_char == '/':
                state = LexState.EOL_COMMENT
        elif car == '#':
            if state == LexState.NORMAL:
                state = LexState.EOL_COMMENT
        elif car == '-':
            if state == LexState.NORMAL and last_char == '-':
                state = LexState.EOL_COMMENT
        elif car == '\n':
            if state == LexState.EOL_COMMENT:
                state = LexState.NORMAL
        elif car == '"':
            if state == LexState.NORMAL:
                state = LexState.STRING
                single_quotes = False
            elif state == LexState.STRING and not single_quotes:
                state = LexState.NORMAL
            elif state == LexState.ESCAPE:
                state = LexState.STRING
        elif car == '\'':
            if state == LexState.NORMAL:
                state = LexState.STRING
                single_quotes = True
            elif state == LexState.STRING and single_quotes:
                state = LexState.NORMAL
            elif state == LexState.ESCAPE:
                state = LexState.STRING
        elif car == '\\':
            if not no_backslash_escapes and state == LexState.STRING:
                state = LexState.ESCAPE
        elif car == ';':
            if state == LexState.NORMAL:
                ending_semicolon = True
        elif car == '?':
            if state == LexState.NORMAL:
                part_list.append(sql[last_parameter_position:i].encode())
                last_parameter_position = i + 1
        elif car == '`':
            if state == LexState.BACKTICK:
                state = LexState.NORMAL
            elif state == LexState.NORMAL:
                state = LexState.BACKTICK
        else:
            if state == LexState.NORMAL and ending_semicolon and ord(car) >= 40:
                ending_semicolon = False
        last_char = car
    if last_parameter_position == 0:
        part_list.append(sql.encode())
    else:
        part_list.append(sql[last_parameter_position:query_length].encode())

    return ClientParser(sql, part_list)
