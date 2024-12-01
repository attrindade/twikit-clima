import asyncio
import pandas as pd
from twikit import Client
from datetime import datetime
import re

def extract_mentions(text):
    return re.findall(r'@\w+', text)

def format_list_to_string(lst):
    """Converte uma lista em string separada por vírgulas"""
    if not lst:
        return ""
    return ", ".join(lst)

def clean_text(text):
    """Limpa o texto removendo quebras de linha e espaços extras"""
    if not text:
        return ""
    return " ".join(text.split())

async def main():
    client = Client('en-US')
    client.load_cookies('cookies.json')
    
    retry_delay = 600
    extracted_user_tweets = []  # Lista para tweets do user 
    original_tweets = []    # Lista para tweets originais dos retweets
    i = 0
    
    user = await client.get_user_by_screen_name('USERNAME') # Coloque aqui o username que os tweets devem ser coletados
    user_id = user.id
    
    print(f"Coletando tweets do usuário @{user.screen_name} (ID: {user_id})")
    
    tweets = await client.get_user_tweets(user_id, 'Tweets', count=40)
    
    while len(tweets) > 0:
        try:        
            print(f"len tweets: {len(tweets)}")
            for tweet in tweets:
                i += 1
                print(f"tweet # {i}")
                
                # Dados para a aba de tweets do user
                user_data = {
                    "tipo_conteudo": "retweet" if tweet.retweeted_tweet else "tweet",
                    "data_publicacao": datetime.strptime(tweet.created_at, '%a %b %d %H:%M:%S %z %Y').strftime('%Y-%m-%d'),
                    "hora_publicacao": datetime.strptime(tweet.created_at, '%a %b %d %H:%M:%S %z %Y').strftime('%H:%M:%S'),
                    "num_likes": tweet.favorite_count,
                    "num_retweets": tweet.retweet_count,
                    "num_replies": tweet.reply_count,
                    "num_views": tweet.view_count,
                    "texto": clean_text(tweet.text),
                    "mencoes": format_list_to_string(extract_mentions(tweet.text)),
                    "hashtags": format_list_to_string(tweet.hashtags),
                    "url": f"https://x.com/{tweet.user.screen_name}/status/{tweet.id}",
                    # "id_tweet": tweet.id,
                    "id_retweet": tweet.retweeted_tweet.id if tweet.retweeted_tweet else None,
                }
                extracted_user_tweets.append(user_data)
                
                # Se for retweet, coleta dados do tweet original
                if tweet.retweeted_tweet:
                    original = tweet.retweeted_tweet
                    original_data = {
                        "id_retweet": original.id,
                        "autor_original": original.user.name,
                        "screen_name_original": original.user.screen_name,
                        "data_original": datetime.strptime(original.created_at, '%a %b %d %H:%M:%S %z %Y').strftime('%Y-%m-%d'),
                        "hora_original": datetime.strptime(original.created_at, '%a %b %d %H:%M:%S %z %Y').strftime('%H:%M:%S'),
                        "texto_original": clean_text(original.text),
                        "mencoes_original": format_list_to_string(extract_mentions(original.text)),
                        "hashtags_original": format_list_to_string(original.hashtags),
                        "num_likes_original": original.favorite_count,
                        "num_retweets_original": original.retweet_count,
                        "num_replies_original": original.reply_count,
                        "num_views_original": original.view_count,
                        "url_original": f"https://x.com/{original.user.screen_name}/status/{original.id}",
                    }
                    original_tweets.append(original_data)
                
                print(f"Conteúdo encontrado: {user_data['data_publicacao']} - {user_data['tipo_conteudo']}")
                
            print("fim de um ciclo de tweets, espera 10s e tenta o next")
            await asyncio.sleep(10)
            tweets = await tweets.next()
            
        except Exception as e:
            print("Aconteceu um erro:")
            print(str(e))
            if "Rate limit exceeded" in str(e):
                print(f"Erro de rate limit, iremos esperar por {retry_delay}s")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
            if retry_delay >= 2400:
                print("retry_delay maior que 40min, PARANDO, MAS SALVANDO")
                break

    print("Coleta finalizada")
    
    # Cria os DataFrames
    df_extracted = pd.DataFrame(extracted_user_tweets)
    df_original = pd.DataFrame(original_tweets)
    
    if not df_extracted.empty:
        # Converte datas para datetime
        df_extracted['data_publicacao'] = pd.to_datetime(df_extracted['data_publicacao'])
        if not df_original.empty:
            df_original['data_original'] = pd.to_datetime(df_original['data_original'])
        
        # Ordena os dataframes
        df_extracted = df_extracted.sort_values(['data_publicacao', 'hora_publicacao'])
        if not df_original.empty:
            df_original = df_original.sort_values(['data_original', 'hora_original'])
        
        # Salva para Excel com múltiplas abas
        with pd.ExcelWriter(f'tweets_{user.screen_name.lower()}_completo.xlsx', engine='openpyxl') as writer:
            df_extracted.to_excel(writer, sheet_name=f'Tweets {user.screen_name}', index=False)
            if not df_original.empty:
                df_original.to_excel(writer, sheet_name='Tweets Originais', index=False)
            
            # Adiciona aba de metadados
            metadata = pd.DataFrame([
                {'campo': 'Data de coleta', 'valor': datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
                {'campo': 'Total de publicações', 'valor': len(df_extracted)},
                {'campo': 'Total de tweets', 'valor': len(df_extracted[df_extracted['tipo_conteudo'] == 'tweet'])},
                {'campo': 'Total de retweets', 'valor': len(df_extracted[df_extracted['tipo_conteudo'] == 'retweet'])},
                {'campo': 'Período inicial', 'valor': df_extracted['data_publicacao'].min().strftime('%Y-%m-%d')},
                {'campo': 'Período final', 'valor': df_extracted['data_publicacao'].max().strftime('%Y-%m-%d')},
                {'campo': 'Usuário coletado', 'valor': f"@{user.screen_name}"},
                {'campo': 'ID do usuário', 'valor': user_id}
            ])
            metadata.to_excel(writer, sheet_name='Metadados', index=False)
        
        print("\nEstatísticas:")
        print(f"Total de publicações: {len(df_extracted)}")
        print(f"Total de tweets: {len(df_extracted[df_extracted['tipo_conteudo'] == 'tweet'])}")
        print(f"Total de retweets: {len(df_extracted[df_extracted['tipo_conteudo'] == 'retweet'])}")
        print(f"Dados salvos em 'tweets_{user.screen_name.lower()}_completo.xlsx'")
    else:
        print("Nenhum conteúdo encontrado!")

asyncio.run(main())