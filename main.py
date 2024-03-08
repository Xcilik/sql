import threading
from pyrogram import Client, filters
from io import BytesIO, StringIO
from sqlalchemy import TEXT, Column, Numeric, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

DB_URI = "postgres://gromkzdt:dzj-tytwpkowrpJ7_kBWuasUp7dP-qJ1@rogue.db.elephantsql.com/gromkzdt"



apa = Client(
    name="sql",
    api_id=21445722,
    api_hash="710f18f90849255dd85837d00d5fe85f",
    bot_token="7144522875:AAEFGYd8mx8p3wvfpBozxW5Ea5L-XEG2y2M",
)




def start() -> scoped_session:
    engine = create_engine(DB_URI, client_encoding="utf8")
    BASE.metadata.bind = engine
    BASE.metadata.create_all(engine)
    return scoped_session(sessionmaker(bind=engine, autoflush=False))


BASE = declarative_base()
SESSION = start()

INSERTION_LOCK = threading.RLock()


class Broadcast(BASE):
    __tablename__ = "broadcast"
    id = Column(Numeric, primary_key=True)
    user_name = Column(TEXT)

    def __init__(self, id, user_name):
        self.id = id
        self.user_name = user_name


Broadcast.__table__.create(checkfirst=True)


#  Add user details -
async def add_user(id, user_name):
    with INSERTION_LOCK:
        msg = SESSION.query(Broadcast).get(id)
        if not msg:
            usr = Broadcast(id, user_name)
            SESSION.add(usr)
            SESSION.commit()


async def delete_user(id):
    with INSERTION_LOCK:
        SESSION.query(Broadcast).filter(Broadcast.id == id).delete()
        SESSION.commit()


async def full_userbase():
    users = SESSION.query(Broadcast).all()
    SESSION.close()
    return users


async def query_msg():
    try:
        return SESSION.query(Broadcast.id).order_by(Broadcast.id)
    finally:
        SESSION.close()





@apa.on_message(filters.command("crot"))
async def test(client, message):
    memek = []
    try:
        query= await query_msg()
        for row in query:
            chat_id = int(row[0])
            memek.append(chat_id)
        final_output = f"{memek}"
        if len(final_output) > 4096:
            filename = "output.txt"
            with open(filename, "w+", encoding="utf8") as out_file:
               out_file.write(str(final_output))
            await message.reply_document(
                document=filename,
                disable_notification=True,
            )
            await apa.send_message(message.chat.id, len(memek))
        else:
            await message.reply(final_output)
    except Exception as e:
        print(str(e))

apa.run()
print("idup")
