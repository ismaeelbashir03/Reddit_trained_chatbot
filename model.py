# importing libraries needed
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch
# importing colour for the bot
from termcolor import colored
# importing os for clearing terminal
import os
# we gte rid of warnings
import transformers
transformers.logging.set_verbosity_error()

# initialising our tokeniser and model
tokenizer = AutoTokenizer.from_pretrained("microsoft/DialoGPT-large")
model = AutoModelForCausalLM.from_pretrained("microsoft/DialoGPT-large")
step = 0

# clearing the screen
os.system('cls' if os.name == 'nt' else 'clear')

# well run the bot until we ctrl c or quit manually
while (True):
    # encode the new user input, add the eos_token and return a tensor in Pytorch
    new_user_input_ids = tokenizer.encode(input(">> User: ") + tokenizer.eos_token, return_tensors='pt')

    # append the new user input tokens to the chat history
    bot_input_ids = torch.cat([chat_history_ids, new_user_input_ids], dim=-1) if step > 0 else new_user_input_ids

    # generated a response while limiting the total chat history to 1000 tokens, 
    chat_history_ids = model.generate(
        bot_input_ids, 
        # we turn on sampling
        do_sample = True,
        max_length=1000, 
        # we also turn on top k and top p
        top_k = 50,
        top_p = 0.95,
        pad_token_id=tokenizer.eos_token_id)

    # pretty print last ouput tokens from bot
    print(colored("DialoGPT: {}".format(tokenizer.decode(chat_history_ids[:, bot_input_ids.shape[-1]:][0], skip_special_tokens=True)),"green"))

    # update our step counter
    step += 1

