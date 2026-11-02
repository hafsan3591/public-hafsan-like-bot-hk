from flask import Flask, request, jsonify
import asyncio
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
import binascii
import aiohttp
import requests
import json
import like_pb2
import uid_generator_pb2
import visit_count_pb2
from google.protobuf.message import DecodeError
from collections import OrderedDict
from datetime import date
import threading

app = Flask(__name__)

# ✅ Valid API keys
VALID_API_KEYS = {
    "hkysxbhyws"
}

# 🇧🇩 Only Bangladesh region allowed
ALLOWED_REGION = "BD"
BD_LIKE_URL = "https://clientbp.ggpolarbear.com/LikeProfile"
BD_INFO_URL = "https://clientbp.ggpolarbear.com/GetPlayerPersonalShow"

# ⚙️ Batch rotation config
# প্রতি ইউজার 100 লাইক: প্রথম 115 টোকেন থেকে 50 করে, তারপর পরের 115 টোকেন থেকে 50 করে
BATCH_SIZE = 115               # এক ব্যাচে 115 টোকেন
TOKEN_DAILY_LIMIT = 150         # প্রতিটা token দিনে 50 বার like
DAILY_LIMIT = 500              # দিনে সর্বোচ্চ 115 user (230 token × 50 like = 11500; 115 user × 100 like = 11500)
TOKEN_FAIL_THRESHOLD = 3       # 5 বার fail হলে token skip

# 📊 State management
used_count = 0
token_usage = {}
token_failures = {}
current_batch = 0
last_reset_date = date.today()
lock = threading.Lock()


def reset_if_new_day():
    global used_count, token_usage, token_failures, current_batch, last_reset_date
    today = date.today()
    if today != last_reset_date:
        used_count = 0
        token_usage = {}
        token_failures = {}
        current_batch = 0
        last_reset_date = today
        app.logger.info("🔄 New day — counters reset")


def load_tokens():
    try:
        with open("token_bd.json", "r") as f:
            return json.load(f)
    except Exception as e:
        app.logger.error(f"Error loading BD tokens: {e}")
        return None


def encrypt_message(plaintext):
    try:
        key = b'Yg&tc%DEuh6%Zc^8'
        iv = b'6oyZDr22E3ychjM%'
        cipher = AES.new(key, AES.MODE_CBC, iv)
        padded_message = pad(plaintext, AES.block_size)
        encrypted_message = cipher.encrypt(padded_message)
        return binascii.hexlify(encrypted_message).decode('utf-8')
    except Exception as e:
        app.logger.error(f"Error encrypting message: {e}")
        return None


def create_protobuf_message(user_id, region):
    try:
        message = like_pb2.like()
        message.uid = int(user_id)
        message.region = region
        return message.SerializeToString()
    except Exception as e:
        app.logger.error(f"Error creating protobuf message: {e}")
        return None


async def send_request(encrypted_uid, token, url):
    try:
        edata = bytes.fromhex(encrypted_uid)
        headers = {
            "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Expect": "100-continue",
            "X-Unity-Version": "2018.4.11f1",
            "X-GA": "v1 1",
            "ReleaseVersion": "OB55"
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=edata, headers=headers) as response:
                text = await response.text()
                status_code = response.status
                return {
                    "token": token,
                    "response": text,
                    "status_code": status_code,
                    "success": status_code == 200
                }
    except Exception as e:
        app.logger.error(f"Exception in send_request: {e}")
        return {"token": token, "response": None, "status_code": 0, "success": False}


def get_available_tokens(tokens):
    """
    প্রথমে current_batch (0 = প্রথম 115 টোকেন) থেকে যেসব টোকেন 50 লাইক হয়নি
    এবং fail হয়নি সেগুলো রিটার্ন করে। ব্যাচ শেষ হলে current_batch বাড়ে।
    """
    global current_batch, token_usage, token_failures

    total_batches = (len(tokens) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_idx in range(current_batch, total_batches):
        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(tokens))
        batch_tokens = []

        for idx in range(start, end):
            used = token_usage.get(idx, 0)
            fails = token_failures.get(idx, 0)
            if used < TOKEN_DAILY_LIMIT and fails < TOKEN_FAIL_THRESHOLD:
                batch_tokens.append((idx, tokens[idx]["token"]))

        if batch_tokens:
            current_batch = batch_idx
            return batch_tokens

    return None


async def send_multiple_requests(uid, region, url):
    """
    একটি ইউজারকে 100 লাইক দিতে হবে:
    - প্রথম 115 টোকেন থেকে 50 করে (প্রতিটি token 50 বার)
    - তারপর পরের 115 টোকেন থেকে 50 করে
    এভাবে দুটি ব্যাচ শেষ হলে 100+ লাইক নিশ্চিত হয়।
    """
    global token_usage, token_failures, current_batch

    protobuf_message = create_protobuf_message(uid, region)
    if protobuf_message is None:
        return None
    encrypted_uid = encrypt_message(protobuf_message)
    if encrypted_uid is None:
        return None

    tokens = load_tokens()
    if not tokens:
        return None

    all_results = []

    # প্রতিটি ইউজারের জন্য 100 লাইক নিশ্চিত করতে我们需要两个批次
    # (প্রতি ব্যাচের টোকেন সর্বোচ্চ 50 লাইক দিতে পারে)
    # যেহেতু প্রতিটি ব্যাচে BATCH_SIZE=115 টোকেন এবং প্রতিটি টোকেন 50 লাইক => এক ব্যাচে 5750 লাইক।
    # 100 লাইকের জন্য এক ব্যাচই যথেষ্ট, কিন্তু আপনি চেয়েছেন প্রথম 115 টোকেন থেকে 50 করে, তারপর পরের 115 থেকে 50 করে।
    # তাই আমরা দুটি ধাপে পাঠাবো: প্রথমে current_batch-এর সব available টোকেন, তারপর পরের ব্যাচ।
    # কিন্তু current_batch স্বয়ংক্রিয়ভাবে বাড়ে। তাই এখানে আমরা সরাসরি দুটি ব্যাচ ধরে নিচ্ছি।

    total_batches = (len(tokens) + BATCH_SIZE - 1) // BATCH_SIZE

    # ইউজারের জন্য 100 লাইক অর্জনের জন্য ধাপে ধাপে ব্যাচ প্রসেস করি
    likes_achieved = 0
    target_likes = 100

    # প্রথমে বর্তমান ব্যাচ থেকে চেষ্টা করি, তারপর পরবর্তী ব্যাচ
    for batch_idx in range(current_batch, total_batches):
        if likes_achieved >= target_likes:
            break

        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(tokens))

        available = []
        for idx in range(start, end):
            used = token_usage.get(idx, 0)
            fails = token_failures.get(idx, 0)
            if used < TOKEN_DAILY_LIMIT and fails < TOKEN_FAIL_THRESHOLD:
                available.append((idx, tokens[idx]["token"]))

        if not available:
            # এই ব্যাচে কোনো টোকেন নেই, পরের ব্যাচে যাই
            if batch_idx == current_batch:
                current_batch = batch_idx + 1
            continue

        # এই ব্যাচের সব available টোকেনে রিকোয়েস্ট পাঠাই
        tasks = [send_request(encrypted_uid, token, url) for idx, token in available]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            idx = available[i][0]
            if isinstance(result, dict) and result.get("success"):
                token_usage[idx] = token_usage.get(idx, 0) + 1
                likes_achieved += 1
            else:
                token_failures[idx] = token_failures.get(idx, 0) + 1
            all_results.append(result)

        # এই ব্যাচের সব টোকেন কি exhausted?
        batch_start = start
        batch_end = end
        all_exhausted = all(
            (token_usage.get(i, 0) >= TOKEN_DAILY_LIMIT or
             token_failures.get(i, 0) >= TOKEN_FAIL_THRESHOLD)
            for i in range(batch_start, batch_end)
        )
        if all_exhausted:
            current_batch = batch_idx + 1
            app.logger.info(f"➡️ Batch {current_batch} এ switch হলো")

        # যদি এই ব্যাচ থেকেই 100 লাইক হয়ে যায়, থামি
        if likes_achieved >= target_likes:
            break

    app.logger.info(f"UID {uid}: মোট {likes_achieved} লাইক দেওয়া হয়েছে")
    return all_results


def create_protobuf(uid):
    try:
        message = uid_generator_pb2.uid_generator()
        message.saturn_ = int(uid)
        message.garena = 1
        return message.SerializeToString()
    except Exception as e:
        app.logger.error(f"Error creating uid protobuf: {e}")
        return None


def enc(uid):
    protobuf_data = create_protobuf(uid)
    if protobuf_data is None:
        return None
    return encrypt_message(protobuf_data)


def make_request(encrypt, region, token):
    try:
        edata = bytes.fromhex(encrypt)
        headers = {
            "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Expect": "100-continue",
            "X-Unity-Version": "2018.4.11f1",
            "X-GA": "v1 1",
            "ReleaseVersion": "OB55"
        }
        response = requests.post(BD_INFO_URL, data=edata, headers=headers, verify=False)
        decoded = visit_count_pb2.Info()
        decoded.ParseFromString(response.content)
        return decoded
    except DecodeError as e:
        app.logger.error(f"DecodeError: {e}")
        return None
    except Exception as e:
        app.logger.error(f"Error in make_request: {e}")
        return None


@app.route('/like', methods=['GET'])
def handle_requests():
    global used_count

    with lock:
        reset_if_new_day()

        api_key = request.args.get("key")
        if api_key not in VALID_API_KEYS:
            result = OrderedDict([("error", "Invalid or missing API key"), ("status", 3)])
            return app.response_class(
                response=json.dumps(result, separators=(',', ':')),
                status=401, mimetype='application/json'
            )

        uid = request.args.get("uid")
        region = ALLOWED_REGION
        if not uid:
            return {"error": "UID is required"}, 400

        try:
            if used_count >= DAILY_LIMIT:
                result = OrderedDict([
                    ("error", "Daily limit reached. Try again after 4:00 AM IST."),
                    ("status", 4), ("daily_limit", DAILY_LIMIT),
                    ("used", used_count), ("remaining", 0)
                ])
                return app.response_class(
                    response=json.dumps(result, separators=(',', ':')),
                    status=429, mimetype='application/json'
                )

            tokens = load_tokens()
            if not tokens:
                raise Exception("Failed to load BD tokens.")
            token = tokens[0]['token']
            encrypted_uid = enc(uid)
            if encrypted_uid is None:
                raise Exception("Encryption of UID failed.")

            before = make_request(encrypted_uid, region, token)
            if before is None:
                raise Exception("Failed to get initial info.")
            before_like = before.AccountInfo.Likes

            asyncio.run(send_multiple_requests(uid, region, BD_LIKE_URL))

            after = make_request(encrypted_uid, region, token)
            if after is None:
                raise Exception("Failed to get final info.")
            after_like = after.AccountInfo.Likes
            like_given = after_like - before_like
            status = 1 if like_given > 0 else 2

            if status == 1:
                used_count += 1

            remaining = max(DAILY_LIMIT - used_count, 0)

            result = OrderedDict([
                ("LikesGivenByAPI", like_given),
                ("LikesafterCommand", after_like),
                ("LikesbeforeCommand", before_like),
                ("PlayerNickname", after.AccountInfo.PlayerNickname),
                ("Level", after.AccountInfo.Levels),
                ("Region", "BD"),
                ("UID", after.AccountInfo.UID),
                ("status", status),
                ("current_batch", current_batch),
                ("daily_limit", DAILY_LIMIT),
                ("used", used_count),
                ("remaining", remaining)
            ])

            return app.response_class(
                response=json.dumps(result, separators=(',', ':')),
                status=200, mimetype='application/json'
            )

        except Exception as e:
            app.logger.error(f"Error: {e}")
            return {"error": str(e)}, 500


@app.route('/remain', methods=['GET'])
def remain_info():
    global used_count, current_batch, token_usage, token_failures
    reset_if_new_day()
    tokens = load_tokens() or []
    total_batches = (len(tokens) + BATCH_SIZE - 1) // BATCH_SIZE
    active_tokens = sum(
        1 for i in range(len(tokens))
        if token_usage.get(i, 0) < TOKEN_DAILY_LIMIT
        and token_failures.get(i, 0) < TOKEN_FAIL_THRESHOLD
    )

    data = {
        "server": "BD",
        "daily_limit": DAILY_LIMIT,
        "remaining_users": max(DAILY_LIMIT - used_count, 0),
        "used": used_count,
        "total_tokens": len(tokens),
        "batch_size": BATCH_SIZE,
        "total_batches": total_batches,
        "current_batch": current_batch,
        "active_tokens_today": active_tokens,
        "token_daily_limit": TOKEN_DAILY_LIMIT,
        "token_fail_threshold": TOKEN_FAIL_THRESHOLD,
        "reset_info": "4:00 AM IST"
    }
    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
