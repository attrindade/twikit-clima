import asyncio
import pandas as pd
from twikit import Client
from datetime import datetime

# Solicita as informações do usuário
USERNAME = '###'
EMAIL = '###@gmail.com'
PASSWORD = '###'

# Solicita as informações para a query
# user_to_search = input("Digite o usuário que deseja buscar (sem @): ")
# since_date = input("Digite a data de início (formato yyyy-mm-dd): ")
# until_date = input("Digite a data de término (formato yyyy-mm-dd): ")

# Compõe a query
query = f'from:obsclima since:2023-01-01 until:2024-01-01'

# Inicializa o cliente
client = Client('en-US')

async def main():
    await client.login(
        auth_info_1=USERNAME,
        auth_info_2=EMAIL,
        password=PASSWORD
    )

    retry_delay = 600

    product = 'Latest'
    
    all_tweets = []
    
    i = 0

    tweets = await client.search_tweet(query, product)
    print("query na busca feita")
    
    while len(tweets) > 0:
        try:        
            print(f"len tweets: {len(tweets)}")
            for tweet in tweets:
                i += 1
                print(f"tweet # {i}")
                tweet_data = {
                    "usuario": tweet.user.name,
                    "data": datetime.strptime(tweet.created_at, '%a %b %d %H:%M:%S %z %Y').strftime('%Y-%m-%d'),
                    "hora": datetime.strptime(tweet.created_at, '%a %b %d %H:%M:%S %z %Y').strftime('%H:%M'),
                    "numlikes": tweet.favorite_count,
                    "numretweet": tweet.retweet_count,
                    "numreplies": tweet.reply_count,
                    "numviews": tweet.view_count,
                    "texto": tweet.text
                }
                all_tweets.append(tweet_data)
            print(tweet_data["data"])
            print("fim de um ciclo de tweets, espera 10s e tenta o next")
            await asyncio.sleep(10)
            tweets = await tweets.next()
        except Exception as e:
            print("Aconteceu um erro:")
            if "Rate limit exceeded" in str(e):
                print(f"Erro de rate limit, iremos esperar por {retry_delay}s")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
            if retry_delay >= 2400:
                print("retry_delay maior que 40min, PARANDO, MAS SALVANDO")
                break

    print("ACABOU 1A PARTE - len tweets era 0, indo a diante")

    # Cria o DataFrame
    df = pd.DataFrame(all_tweets)
    
    # Salva para Excel
    file_name = f"tweets_obsclima_23.xlsx"
    df.to_excel(file_name, index=False)
    print(f"Saved to {file_name}")

asyncio.run(main())
